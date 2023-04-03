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

static PyObject* _PyBytes_FromAddrAndLen(char* addr, size_t len) {
    if (len == 0 || len == SIZE_MAX) {
        return PyBytes_FromString("");
    }

    PyObject* result = PyBytes_FromStringAndSize(addr, len);

    return result;
}


static void _IndexAddr_ToDisk(size_t value, size_t* write_count, FILE* index_file, LazyCSV_IndexCounts* index_counts) {
    if (*write_count == SIZE_MAX) {
        *write_count = 0;
    }
    else {
        value -= *write_count;
        *write_count += 1;
    }

    if (value <= UCHAR_MAX) {
        unsigned char item = (unsigned char)value;
        fwrite(&item, sizeof(char), 1, index_file);
        index_counts->chars += 1;
    }
    else if (value <= USHRT_MAX) {
        unsigned short item = (unsigned short)value;
        fwrite(&item, sizeof(short), 1, index_file);
        index_counts->shorts += 1;
    }
    else if (value <= ULONG_MAX) {
        unsigned long item = (unsigned long)value;
        fwrite(&item, sizeof(long), 1, index_file);
        index_counts->longs += 1;
    }
    else {
        fwrite(&value, sizeof(size_t), 1, index_file);
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

        size_t offset = (position*(lazy->cols+1)) + iter->col;

        LazyCSV_IndexCounts* counts = lazy->_index->index_counts;

        size_t cs = _DataAddr_FromIndex(index, offset, counts);
        size_t ce = _DataAddr_FromIndex(index, offset+1, counts);

        char* addr = data + cs + offset;
        size_t len = ce - cs;

        char strip_quotes = (
            lazy->_unquote
            && addr[0] == DOUBLE_QUOTE
            && addr[len-1] == DOUBLE_QUOTE
        );

        if (strip_quotes) {
            addr = addr+1;
            len = len-2;
        }

        result = _PyBytes_FromAddrAndLen(addr, len);
    }

    else {
        PyErr_SetNone(PyExc_StopIteration);
    }

    iter->position += 1;
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

        size_t offset =
            ((iter->row+!lazy->_skip_headers)*(lazy->cols+1)) + position;

        LazyCSV_IndexCounts* counts = lazy->_index->index_counts;

        size_t cs = _DataAddr_FromIndex(index, offset, counts);
        size_t ce = _DataAddr_FromIndex(index, offset+1, counts);

        char* addr = data + cs + offset;
        size_t len = ce - cs;

        char strip_quotes = (
            lazy->_unquote
            && addr[0] == DOUBLE_QUOTE
            && addr[len-1] == DOUBLE_QUOTE
        );

        if (strip_quotes) {
            addr = addr+1;
            len = len-2;
        }

        result = _PyBytes_FromAddrAndLen(addr, len);

    }
    else {
        PyErr_SetNone(PyExc_StopIteration);
    }
    iter->position += 1;

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
    return NULL;
}


static PyObject* LazyCSV_IterSelf(PyObject* self) {
    Py_INCREF(self);
    return self;
}


static void LazyCSV_IterDestruct(LazyCSV_Iter* self) {
    Py_DECREF(self->lazy);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyTypeObject LazyCSV_IterType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "lazycsv_iterator",
    .tp_itemsize = sizeof(LazyCSV_Iter),
    .tp_dealloc = (destructor)LazyCSV_IterDestruct,
    .tp_flags = Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,
    .tp_doc = "LazyCSV iterable",
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

    static char* kwlist[] = {"", "skip_headers", "unquote", NULL};

    char ok = PyArg_ParseTupleAndKeywords(
        args, kwargs, "s|pp", kwlist, &fullname,
        &skip_headers, &unquote
    );

    if (!ok) {
        PyErr_SetString(
            PyExc_ValueError,
            "unable to parse function arguments"
        );
        return NULL;
    }

    int ufd = open(fullname, O_RDWR);
    if (ufd == -1) {
        PyErr_SetString(
            PyExc_FileNotFoundError,
            "unable to open file, requires absolute path"
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

    int mmap_flags = PROT_READ|PROT_WRITE;
    char* file = mmap(NULL, file_len, mmap_flags, MAP_SHARED, ufd, 0);

    PyObject* tempdir; char* dirname;
    _TempDir_AsString(&tempdir, &dirname);

    char* index_name = tempnam(dirname, "Lazy_");
    FILE* index_file = fopen(index_name, "wb+");

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
            _IndexAddr_ToDisk(val, &write_count, index_file, index_counts);
        }

        if (c == DOUBLE_QUOTE) {
            quoted = !quoted;
        }

        else if (!quoted && c == COMMA) {
            _IndexAddr_ToDisk(i, &write_count, index_file, index_counts);
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
                _IndexAddr_ToDisk(i, &write_count, index_file, index_counts);
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
                    _IndexAddr_ToDisk(i, &write_count, index_file, index_counts);
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
        _IndexAddr_ToDisk(file_len, &write_count, index_file, index_counts);
    }

    rows = row_index - overcount + skip_headers;
    cols = cols + 1;

    fflush(index_file);
    fclose(index_file);

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

    char* index = mmap(NULL, ist.st_size, mmap_flags, MAP_SHARED, ifd, 0);

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
    Py_DECREF(self->_index->dir);
    free(self->_data);
    free(self->_index);
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

