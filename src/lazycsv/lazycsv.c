#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>

#include <Python.h>
#include "structmember.h"

#define DOUBLE_QUOTE 34
#define COMMA 44
#define LINE_FEED 10
#define CARRIAGE_RETURN 13

void PyDebug() {return;}


typedef struct {
    char* data;
    Py_ssize_t size;
    Py_ssize_t capacity;
} WriteBuffer;


typedef struct {
    PyObject* empty;
} LazyCSV_Cache;


typedef struct {
    size_t chars;
    size_t shorts;
    size_t longs;
    size_t size_ts;
} LazyCSV_IndexCounts;


typedef struct {
    int fd;
    char* name;
    PyObject* dir;
    char* index;
    struct stat st;
    LazyCSV_IndexCounts* index_counts;
} LazyCSV_Index;


typedef struct {
    int fd;
    char* data;
    struct stat st;
} LazyCSV_Data;


typedef struct {
    PyObject_HEAD
    PyObject* headers;
    size_t rows;
    size_t cols;
    char* name;
    int _skip_headers;
    int _unquote;
    LazyCSV_Index* _index;
    LazyCSV_Data* _data;
    LazyCSV_Cache* _cache;
} LazyCSV;


typedef struct {
    PyObject_HEAD
    PyObject* lazy;
    size_t row;
    size_t col;
    size_t position;
    char reversed;
    char skip_header;
} LazyCSV_Iter;


static LazyCSV_IndexCounts* _IndexCounts_Init() {
    LazyCSV_IndexCounts* index_counts = malloc(sizeof(LazyCSV_IndexCounts));
    index_counts->chars = 0;
    index_counts->shorts = 0;
    index_counts->longs = 0;
    index_counts->size_ts = 0;
    return index_counts;
}


static void _BufferWrite(int index_file, WriteBuffer* buffer, void* data, size_t size) {
    if (buffer->size + (Py_ssize_t)size >= buffer->capacity) {
        write(index_file, buffer->data, buffer->size);
        buffer->size = 0;
    }
    memcpy(&buffer->data[buffer->size], data, size);
    buffer->size += size;
}


static void _FlushBuffer(int index_file, WriteBuffer* buffer) {
    write(index_file, buffer->data, buffer->size);
    buffer->size = 0;
    fsync(index_file);
}

static void _IndexAddr_ToDisk(
    size_t value, size_t* write_count, int index_file,
    LazyCSV_IndexCounts* index_counts, WriteBuffer* buffer
) {
    if (*write_count == SIZE_MAX) {
        *write_count = 0;
    }
    else {
        value -= *write_count;
        *write_count += 1;
    }

    if (value <= UCHAR_MAX) {
        unsigned char item = (unsigned char)value;
        _BufferWrite(index_file, buffer, &item, sizeof(unsigned char));
        index_counts->chars += 1;
    }
    else if (value <= USHRT_MAX) {
        unsigned short item = (unsigned short)value;
        _BufferWrite(index_file, buffer, &item, sizeof(unsigned short));
        index_counts->shorts += 1;
    }
    else if (value <= ULONG_MAX) {
        unsigned long item = (unsigned long)value;
        _BufferWrite(index_file, buffer, &item, sizeof(unsigned long));
        index_counts->longs += 1;
    }
    else {
        _BufferWrite(index_file, buffer, &value, sizeof(size_t));
        index_counts->size_ts += 1;
    }
}


static size_t _DataAddr_FromIndex(char* index, size_t value, LazyCSV_IndexCounts* index_counts) {
    size_t offset;
    size_t chars = index_counts->chars;
    size_t shorts = index_counts->shorts;
    size_t longs = index_counts->longs;

    if (value < chars) {
        offset = value * sizeof(unsigned char);
        size_t result = *(unsigned char*)(index+offset);
        return result;
    }

    value -= chars;

    if (value < shorts) {
        offset = (
            (chars * sizeof(char))
            + (value * sizeof(short))
        );
        return *(unsigned short*)(index+offset);
    }

    value -= shorts;

    if (value < longs) {
        offset = (
            (chars * sizeof(char))
            + (shorts * sizeof(short))
            + (value * sizeof(long))
        );
        return *(unsigned long*)(index+offset);
    }

    value -= longs;

    offset = (
        (chars * sizeof(char))
        + (shorts * sizeof(short))
        + (longs * sizeof(long))
        + (value * sizeof(size_t))
    );

    return *(size_t*)(index+offset);
}


static PyObject* LazyCSV_IterCol(LazyCSV_Iter* iter) {
    LazyCSV* lazy = (LazyCSV*)iter->lazy;

    char* index = lazy->_index->index;
    char* data = lazy->_data->data;

    PyObject* result = NULL;

    if (iter->position < lazy->rows) {

        size_t position;
        if (iter->reversed) {
            position = lazy->rows - 1 - iter->position + !lazy->_skip_headers;
        }
        else {
            position = iter->position + !lazy->_skip_headers;
        }

        iter->position += 1;

        size_t offset = (position*(lazy->cols+1)) + iter->col;

        LazyCSV_IndexCounts* counts = lazy->_index->index_counts;

        size_t cs = _DataAddr_FromIndex(index, offset, counts);
        size_t ce = _DataAddr_FromIndex(index, offset+1, counts);

        size_t len = ce - cs;

        if (len == 0 || len == SIZE_MAX) {
            // short circuit if result is empty string
            // (len == SIZE_MAX due to how we index \r\n)
            result = lazy->_cache->empty;
            Py_INCREF(result);
            return result;
        }

        char* addr = data + cs + offset;

        char strip_quotes = (
            lazy->_unquote
            && addr[0] == DOUBLE_QUOTE
            && addr[len-1] == DOUBLE_QUOTE
        );

        if (strip_quotes) {
            addr = addr+1;
            len = len-2;
        }

        result = PyBytes_FromStringAndSize(addr, len);
    }

    else {
        PyErr_SetNone(PyExc_StopIteration);
    }

    return result;
}


static PyObject* LazyCSV_IterRow(LazyCSV_Iter* iter) {
    LazyCSV* lazy = (LazyCSV*)iter->lazy;

    char* index = lazy->_index->index;
    char* data = lazy->_data->data;

    PyObject* result = NULL;

    if (iter->position < lazy->cols) {
        size_t position = iter->reversed ?
            lazy->cols - iter->position - 1 : iter->position;

        iter->position += 1;

        size_t offset =
            ((iter->row+!lazy->_skip_headers)*(lazy->cols+1)) + position;

        LazyCSV_IndexCounts* counts = lazy->_index->index_counts;

        size_t cs = _DataAddr_FromIndex(index, offset, counts);
        size_t ce = _DataAddr_FromIndex(index, offset+1, counts);

        size_t len = ce - cs;

        if (len == 0 || len == SIZE_MAX) {
            // short circuit if result is empty string
            // (len == SIZE_MAX due to how we index \r\n)
            result = lazy->_cache->empty;
            Py_INCREF(result);
            return result;
        }

        char* addr = data + cs + offset;

        char strip_quotes = (
            lazy->_unquote
            && addr[0] == DOUBLE_QUOTE
            && addr[len-1] == DOUBLE_QUOTE
        );

        if (strip_quotes) {
            addr = addr+1;
            len = len-2;
        }

        result = PyBytes_FromStringAndSize(addr, len);
    }
    else {
        PyErr_SetNone(PyExc_StopIteration);
    }

    return result;
}


static PyObject* LazyCSV_IterNext(PyObject* self) {
    LazyCSV_Iter* iter = (LazyCSV_Iter*)self;
    if (iter->col != SIZE_MAX) {
        return LazyCSV_IterCol(iter);
    }
    if (iter->row != SIZE_MAX) {
        return LazyCSV_IterRow(iter);
    }
    PyErr_SetString(
        PyExc_RuntimeError,
        "could not determine axis for materialization"
    );
    return NULL;
}


static PyObject* LazyCSV_IterSelf(PyObject* self) {
    Py_INCREF(self);
    return self;
}


static inline void _PyTuple_SET_ITEM(PyObject* coll, Py_ssize_t idx, PyObject* item) {
    // needs to be actual fn depending on compiler
    PyTuple_SET_ITEM(coll, idx, item);
}


static inline void _PyList_SET_ITEM(PyObject* coll, Py_ssize_t idx, PyObject* item) {
    // needs to be actual fn depending on compiler
    PyList_SET_ITEM(coll, idx, item);
}


static PyObject* LazyCSV_IterMater(PyObject* self, PyObject* args, PyObject* kwargs) {
    LazyCSV_Iter* iter = (LazyCSV_Iter*)self;
    LazyCSV* lazy = (LazyCSV*)iter->lazy;

    PyTypeObject* type = NULL;

    static char *kwlist[] = {"type", NULL};
    char ok = PyArg_ParseTupleAndKeywords(
        args, kwargs, "|O", kwlist, &type
    );

    if (!ok) {
        PyErr_SetString(
            PyExc_ValueError,
            "unable to parse iter.materialize(type=list) arguments"
        );
        return NULL;
    }

    PyObject* (*next)(LazyCSV_Iter*);
    PyObject* (*new)(Py_ssize_t);
    void (*set)(PyObject*, Py_ssize_t, PyObject*);

    if (!type || type->tp_new == (&PyList_Type)->tp_new) {
        new = PyList_New;
        set = _PyList_SET_ITEM;
    }
    else if (type->tp_new == (&PyTuple_Type)->tp_new) {
        new = PyTuple_New;
        set = _PyTuple_SET_ITEM;
    }
    else {
        PyErr_SetString(
            PyExc_RuntimeError,
            "could not determine type for materialization,"
            " should be either list or tuple"
        );
        return NULL;
    }

    size_t size;

    if (iter->col != SIZE_MAX) {
        size = lazy->rows - iter->position;
        next = LazyCSV_IterCol;
    }
    else if (iter->row != SIZE_MAX) {
        size = lazy->cols - iter->position;
        next = LazyCSV_IterRow;
    }
    else {
        PyErr_SetString(
            PyExc_RuntimeError,
            "could not determine axis for materialization"
        );
        return NULL;
    }

    PyObject* result = new(size);

    PyObject* item;
    size_t index = 0;
    while ((item = next(iter))) {
        set(result, index, item);
        index++;
    }

    if (PyErr_ExceptionMatches(PyExc_StopIteration)) {
        PyErr_Clear();
    }

    return result;
}


static void LazyCSV_IterDestruct(LazyCSV_Iter* self) {
    Py_DECREF(self->lazy);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyMethodDef LazyCSV_IterMethods[] = {
    {
        "materialize",
        (PyCFunction)LazyCSV_IterMater,
        METH_VARARGS|METH_KEYWORDS,
        "materialize iterator into a list"
    },
    {NULL, }
};


static PyTypeObject LazyCSV_IterType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "lazycsv_iterator",
    .tp_itemsize = sizeof(LazyCSV_Iter),
    .tp_dealloc = (destructor)LazyCSV_IterDestruct,
    .tp_flags = Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,
    .tp_doc = "LazyCSV iterable",
    .tp_methods = LazyCSV_IterMethods,
    .tp_iter = LazyCSV_IterSelf,
    .tp_iternext = LazyCSV_IterNext,
};


static void _TempDir_AsString(PyObject** tempdir, char** dirname) {
    PyObject* tempfile = PyImport_ImportModule("tempfile");
    PyObject* tempdir_obj = PyObject_GetAttrString(
        tempfile, "TemporaryDirectory"
    );

    *tempdir = PyObject_CallObject(tempdir_obj, NULL);
    PyObject* dirname_obj = PyObject_GetAttrString(*tempdir, "name");
    PyObject* dirstring = PyUnicode_AsUTF8String(dirname_obj);
    *dirname = PyBytes_AsString(dirstring);

    Py_DECREF(tempfile);
    Py_DECREF(tempdir_obj);
    Py_DECREF(dirname_obj);
    Py_DECREF(dirstring);
}


static PyObject* LazyCSV_New(
        PyTypeObject* type, PyObject* args, PyObject* kwargs
) {

    char* fullname;
    int skip_headers = 0;
    int unquote = 1;
    Py_ssize_t buffer_capacity = 2097152; // 2**21
    char* dirname;

    static char* kwlist[] = {
        "", "skip_headers", "unquote", "buffer_size", "index_dir", NULL
    };

    char ok = PyArg_ParseTupleAndKeywords(
        args, kwargs, "s|ppns", kwlist, &fullname,
        &skip_headers, &unquote, &buffer_capacity, &dirname
    );

    if (!ok) {
        PyErr_SetString(
            PyExc_ValueError,
            "unable to parse function arguments"
        );
        return NULL;
    }

    if (buffer_capacity < 0) {
        PyErr_SetString(
            PyExc_ValueError,
            "buffer size cannot be less than 0"
        );
        return NULL;
    }

    int ufd = open(fullname, O_RDONLY);
    if (ufd == -1) {
        PyErr_SetString(
            PyExc_FileNotFoundError,
            "unable to open data file,"
            " check to be sure that the user has read permissions"
            " and/or ownership of the file, that the file exists."
        );
        return NULL;
    }

    struct stat ust;
    if (fstat(ufd, &ust) < 0) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "unable to stat user file"
        );
        return NULL;
    }

    size_t file_len = ust.st_size;

    int mmap_flags = PROT_READ;
    char* file = mmap(NULL, file_len, mmap_flags, MAP_PRIVATE, ufd, 0);

    PyObject* tempdir = NULL;
    if (!dirname) {
        _TempDir_AsString(&tempdir, &dirname);
    }

    char* index_name = tempnam(dirname, "Lazy_");

    int index_file = open(index_name, O_WRONLY|O_CREAT|O_APPEND, S_IRWXU);

    char quoted = 0, c, cm1 = LINE_FEED, cm2 = 0;
    size_t rows = 0, cols = 0, row_index = 0, col_index = 0;

    int newline = -1;

    // overflow happens when a row has more columns than the header row,
    // if this happens during the parse, the comma of the nth col will indicate
    // the line ending. Underflow happens when a row has less columns than the
    // header row, and missing values will be appended to the row as an empty
    // field.

    size_t overflow = SIZE_MAX;
    char *overflow_warning = NULL, *underflow_warning = NULL;

    LazyCSV_IndexCounts* index_counts = _IndexCounts_Init();

    size_t write_count = SIZE_MAX;

    char* buffer_data = malloc(buffer_capacity);
    WriteBuffer buffer = {
        buffer_data,
        0,
        buffer_capacity
    };

    for (size_t i = 0; i < file_len; i++) {
        if (overflow != SIZE_MAX) {
            if (i < overflow) {
                continue;
            }
        }

        c = file[i];

        if (col_index == 0
            && (cm1 == LINE_FEED || cm1 == CARRIAGE_RETURN)
            && cm2 != CARRIAGE_RETURN) {
            size_t val = (
                i == 0 || newline == (CARRIAGE_RETURN+LINE_FEED)
            ) ? i : i - 1;
            _IndexAddr_ToDisk(
                val, &write_count, index_file, index_counts, &buffer
            );
        }

        if (c == DOUBLE_QUOTE) {
            quoted = !quoted;
        }

        else if (!quoted && c == COMMA) {
            _IndexAddr_ToDisk(i, &write_count, index_file, index_counts, &buffer);
            if (cols == 0 || col_index < cols) {
                col_index += 1;
            }
            else {
                overflow_warning =
                    "column overflow encountered while parsing CSV, "
                    "extra values will be truncated!";
                overflow = i;
                for (;;) {
                    if (file[overflow] == LINE_FEED
                        || file[overflow] == CARRIAGE_RETURN) {
                        break;
                    }
                    else if (overflow >= file_len) {
                        break;
                    }
                    overflow += 1;
                }
            }
        }

        else if (!quoted && c == LINE_FEED && cm1 == CARRIAGE_RETURN) {
            // no-op, don't match next block for \r\n
        }

        else if (!quoted && (c == CARRIAGE_RETURN || c == LINE_FEED)) {
            if (overflow == SIZE_MAX) {
                _IndexAddr_ToDisk(i, &write_count, index_file, index_counts, &buffer);
            }
            else {
                overflow = SIZE_MAX;
            }

            if (row_index == 0) {
                cols = col_index;
            }
            else if (col_index < cols && col_index != 0) {
                underflow_warning =
                    "column underflow encountered while parsing CSV, "
                    "missing values will be filled with the empty bytestring!";
                while (col_index < cols) {
                    _IndexAddr_ToDisk(i, &write_count, index_file, index_counts, &buffer);
                    col_index += 1;
                }
            }

            if (newline == -1) {
                newline = file[i+1] == LINE_FEED ?
                    LINE_FEED + CARRIAGE_RETURN : c;
            }

            col_index = 0;
            row_index += 1;
        }

        cm2 = cm1;
        cm1 = c;
    }

    size_t lf = 0;
    size_t cr = 0;
    size_t overcount = 0;

    // run backwards through the file and remove the potential EOF \n from
    // the row count.
    for (size_t j = file_len - 1; j > 0; j--) {
        c = file[j];
        if (c == CARRIAGE_RETURN) {
            lf += 1;
        }
        else if (c == LINE_FEED) {
            cr += 1;
        }
        else break;
    }

    overcount = lf >= cr ? lf : cr;

    if (!overcount) {
        _IndexAddr_ToDisk(file_len, &write_count, index_file, index_counts, &buffer);
    }

    rows = row_index - overcount + skip_headers;
    cols = cols + 1;

    _FlushBuffer(index_file, &buffer);
    close(index_file);
    free(buffer_data);

    if (overflow_warning)
        PyErr_WarnEx(
            PyExc_RuntimeWarning,
            overflow_warning,
            2
        );

    if (underflow_warning)
        PyErr_WarnEx(
            PyExc_RuntimeWarning,
            underflow_warning,
            1
        );

    int ifd = open(index_name, O_RDWR);
    struct stat ist;
    if (fstat(ifd, &ist) < 0) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "unable to stat index file"
        );
        return NULL;
    }

    char* index = mmap(NULL, ist.st_size, mmap_flags, MAP_PRIVATE, ifd, 0);

    PyObject* headers;
    if (!skip_headers) {
        headers = PyTuple_New(cols);
        size_t cs, ce;
        for (size_t i = 0; i < cols; i++) {
            cs = _DataAddr_FromIndex(index, i, index_counts);
            ce = _DataAddr_FromIndex(index, i+1, index_counts);
            if (cs == ce) {
                PyTuple_SET_ITEM(headers, i, PyBytes_FromString(""));
            }
            else {
                char* addr = i == 0 ? file : file + cs + i;
                size_t len = ce - cs;
                if (unquote
                        && addr[0] == DOUBLE_QUOTE
                        && addr[len-1] == DOUBLE_QUOTE) {
                    addr = addr+1;
                    len = len-2;
                }
                PyTuple_SET_ITEM(
                    headers, i, PyBytes_FromStringAndSize(addr, len)
                );
            }
        }
    }
    else {
        headers = PyTuple_New(0);
    }

    LazyCSV_Cache* _cache = malloc(sizeof(LazyCSV_Cache));
    _cache->empty = PyBytes_FromString("");

    LazyCSV_Index* _index = malloc(sizeof(LazyCSV_Index));

    _index->name = index_name;
    _index->dir = tempdir;
    _index->fd = ifd;
    _index->index = index;
    _index->st = ist;
    _index->index_counts = index_counts;

    LazyCSV_Data* _data = malloc(sizeof(LazyCSV_Data));

    _data->fd = ufd;
    _data->data = file;
    _data->st = ust;

    LazyCSV* self = (LazyCSV*)type->tp_alloc(type, 0);
    if (!self) {
        PyErr_SetString(
            PyExc_MemoryError,
            "unable to allocate LazyCSV object"
        );
        return NULL;
    }

    self->rows = rows;
    self->cols = cols;
    self->name = fullname;
    self->headers = headers;
    self->_skip_headers = skip_headers;
    self->_unquote = unquote;
    self->_index = _index;
    self->_data = _data;
    self->_cache = _cache;

    return (PyObject*)self;
}


static PyObject* LazyCSV_Seq(
        PyObject* self, PyObject* args, PyObject* kwargs
) {

    size_t row = SIZE_MAX;
    size_t col = SIZE_MAX;
    char reversed;

    static char *kwlist[] = {"row", "col", "reversed", NULL};

    char ok = PyArg_ParseTupleAndKeywords(
        args, kwargs, "|nnb", kwlist, &row, &col, &reversed
    );

    if (!ok) {
        PyErr_SetString(
            PyExc_ValueError,
            "unable to parse lazy.sequence() arguments"
        );
        return NULL;
    }

    if (row == SIZE_MAX && col == SIZE_MAX) {
        PyErr_SetString(
            PyExc_ValueError,
            "a row or a col value is required"
        );
        return NULL;
    }

    if (row != SIZE_MAX && col != SIZE_MAX) {
        PyErr_SetString(
            PyExc_ValueError,
            "cannot specify both row and col"
        );
        return NULL;
    }

    PyTypeObject* type = &LazyCSV_IterType;
    LazyCSV_Iter* iter = (LazyCSV_Iter*)type->tp_alloc(type, 0);

    if (!iter)
        return NULL;

    iter->row = row;
    iter->col = col;
    iter->reversed = reversed;
    iter->position = 0;
    iter->lazy = self;
    Py_INCREF(self);

    return (PyObject*)iter;
}


static void LazyCSV_Destruct(LazyCSV* self) {
    munmap(self->_data->data, self->_data->st.st_size);
    munmap(self->_index->index, self->_index->st.st_size);
    close(self->_data->fd);
    close(self->_index->fd);
    remove(self->_index->name);
    free(self->_index->name);
    free(self->_index->index_counts);
    Py_XDECREF(self->_index->dir);
    Py_DECREF(self->_cache->empty);
    free(self->_data);
    free(self->_index);
    free(self->_cache);
    Py_DECREF(self->headers);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyMemberDef LazyCSVMembers[] = {
    {"headers", T_OBJECT, offsetof(LazyCSV, headers), READONLY, "header tuple"},
    {"rows", T_LONG, offsetof(LazyCSV, rows), READONLY, "row length"},
    {"cols", T_LONG, offsetof(LazyCSV, cols), READONLY, "col length"},
    {"name", T_STRING, offsetof(LazyCSV, name), READONLY, "file name"},
    {NULL, }
};


static PyMethodDef LazyCSVMethods[] = {
    {
        "sequence",
        (PyCFunction)LazyCSV_Seq,
        METH_VARARGS|METH_KEYWORDS,
        "get column iterator"
    },
    {NULL, }
};


static PyTypeObject LazyCSVType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "lazycsv.LazyCSV",
    .tp_basicsize = sizeof(LazyCSV),
    .tp_dealloc = (destructor)LazyCSV_Destruct,
    .tp_flags = Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,
    .tp_methods = LazyCSVMethods,
    .tp_members = LazyCSVMembers,
    .tp_new = LazyCSV_New,
};


static PyModuleDef LazyCSVModule = {
    PyModuleDef_HEAD_INIT,
    "lazycsv",
    "module for custom lazycsv object",
    -1,
    NULL
};


PyMODINIT_FUNC PyInit_lazycsv() {
    if (PyType_Ready(&LazyCSVType) < 0)
        return NULL;

    if (PyType_Ready(&LazyCSV_IterType) < 0)
        return NULL;

    PyObject* module = PyModule_Create(&LazyCSVModule);
    if (!module)
        return NULL;

    Py_INCREF(&LazyCSVType);
    PyModule_AddObject(module, "LazyCSV", (PyObject*)&LazyCSVType);
    return module;
}

