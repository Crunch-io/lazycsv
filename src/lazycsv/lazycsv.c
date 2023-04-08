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
    PyObject** items;
} LazyCSV_Cache;


typedef struct {
    size_t index;
    size_t chars;
    size_t shorts;
    size_t longs;
    size_t size_ts;
} LazyCSV_IndexCounts;


typedef struct {
    int fd;
    struct stat st;
    char* name;
    char* data;
} LazyCSV_IndexFile;


typedef struct {
    PyObject* dir;
    LazyCSV_IndexFile* commas;
    LazyCSV_IndexFile* newlines;
} LazyCSV_Index;


typedef struct {
    int fd;
    char* name;
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
    char _newline;
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


static void _BufferWrite(
    int fd, WriteBuffer* buffer, void* data, size_t size
) {
    if (buffer->size + (Py_ssize_t)size >= buffer->capacity) {
        write(fd, buffer->data, buffer->size);
        buffer->size = 0;
    }
    memcpy(&buffer->data[buffer->size], data, size);
    buffer->size += size;
}


static void _BufferFlush(int comma_file, WriteBuffer* buffer) {
    write(comma_file, buffer->data, buffer->size);
    buffer->size = 0;
    fsync(comma_file);
}

static size_t _Value_ToDisk(
    size_t value, int fd,
    LazyCSV_IndexCounts* counts, WriteBuffer* buffer
) {

    size_t write_size;
    value = counts->index > value ? 0 : value - counts->index;

    if (value <= UCHAR_MAX) {
        write_size = sizeof(char);
        unsigned char item = (unsigned char)value;
        _BufferWrite(fd, buffer, &item, write_size);
        counts->chars += 1;
    }
    else if (value <= USHRT_MAX) {
        write_size = sizeof(short);
        unsigned short item = (unsigned short)value;
        _BufferWrite(fd, buffer, &item, write_size);
        counts->shorts += 1;
    }
    else if (value <= ULONG_MAX) {
        write_size = sizeof(long);
        unsigned long item = (unsigned long)value;
        _BufferWrite(fd, buffer, &item, write_size);
        counts->longs += 1;
    }
    else {
        write_size = sizeof(size_t);
        _BufferWrite(fd, buffer, &value, write_size);
        counts->size_ts += 1;
    }

    return write_size;
}


static size_t _Value_FromIndex(
    char* index, size_t value, LazyCSV_IndexCounts* counts
) {
    size_t offset;
    size_t chars = counts->chars;
    size_t shorts = counts->shorts;
    size_t longs = counts->longs;

    if (value < chars) {
        offset = value * sizeof(unsigned char);
        return *(unsigned char*)(index+offset);
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

        char* newlines = lazy->_index->newlines->data;
        char* commas = lazy->_index->commas->data;

        LazyCSV_IndexCounts* newlines_index =
            (LazyCSV_IndexCounts*)
            (newlines + position*sizeof(LazyCSV_IndexCounts));

        size_t newline_offset = newlines_index->index;

        char* index = (char*)(commas+newline_offset);

        size_t cs = _Value_FromIndex(
            index,
            iter->col,
            newlines_index
        );

        size_t ce = _Value_FromIndex(
            index,
            iter->col+1,
            newlines_index
        );

        size_t len = ce - cs - 1;

        if (len == 0 || len == SIZE_MAX) {
            result = lazy->_cache->empty;
            Py_INCREF(result);
        }
        else if (len == 1) {
            // short circuit to cache
            char* addr = lazy->_data->data + newline_offset + cs;
            result = lazy->_cache->items[(size_t)*addr];
            Py_INCREF(result);
        }
        else {
            char* addr = lazy->_data->data + newline_offset + cs;

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
    }
    else {
        PyErr_SetNone(PyExc_StopIteration);
    }

    return result;
}


static PyObject* LazyCSV_IterRow(LazyCSV_Iter* iter) {
    LazyCSV* lazy = (LazyCSV*)iter->lazy;

    PyObject* result = NULL;

    if (iter->position < lazy->cols) {
        size_t position = iter->reversed ?
            lazy->cols - iter->position - 1 : iter->position;

        iter->position += 1;

        char* newlines = lazy->_index->newlines->data;
        char* commas = lazy->_index->commas->data;

        size_t row = iter->row + !lazy->_skip_headers;

        LazyCSV_IndexCounts* newlines_index =
            (LazyCSV_IndexCounts*)
            (newlines + row*sizeof(LazyCSV_IndexCounts));

        size_t newline_offset = newlines_index->index;

        char* index = (char*)(commas+newline_offset);

        size_t cs = _Value_FromIndex(
            index,
            position,
            newlines_index
        );

        size_t ce = _Value_FromIndex(
            index,
            position+1,
            newlines_index
        );

        size_t len = ce - cs - 1;

        if (len == 0 || len == SIZE_MAX) {
            // short circuit if result is empty string
            result = lazy->_cache->empty;
            Py_INCREF(result);
        }
        else if (len == 1) {
            // short circuit to cache
            char* addr = lazy->_data->data + newline_offset + cs;
            result = lazy->_cache->items[(size_t)*addr];
            Py_INCREF(result);
        }
        else {
            char* addr = lazy->_data->data + newline_offset + cs;

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

    char* comma_index = tempnam(dirname, "Lazy_");
    char* newline_index = tempnam(dirname, "Lazy_");

    int comma_file = open(comma_index, O_WRONLY|O_CREAT|O_APPEND, S_IRWXU);
    int newline_file = open(newline_index, O_WRONLY|O_CREAT|O_APPEND, S_IRWXU);

    char quoted = 0, c, cm1 = LINE_FEED, cm2 = 0;
    size_t rows = 0, cols = SIZE_MAX, row_index = 0, col_index = 0;

    int newline = -1;

    // overflow happens when a row has more columns than the header row,
    // if this happens during the parse, the comma of the nth col will indicate
    // the line ending. Underflow happens when a row has less columns than the
    // header row, and missing values will be appended to the row as an empty
    // field.

    size_t overflow = SIZE_MAX;
    char *overflow_warning = NULL, *underflow_warning = NULL;

    LazyCSV_IndexCounts line_counts;

    WriteBuffer comma_buffer = {malloc(buffer_capacity), 0, buffer_capacity};
    WriteBuffer newline_buffer = {malloc(buffer_capacity), 0, buffer_capacity};

    size_t comma_count=0;

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
                newline == (CARRIAGE_RETURN+LINE_FEED)
            ) ? i + 1 : i;

            line_counts = (LazyCSV_IndexCounts){
                .index=comma_count, .chars=0, .shorts=0, .longs=0, .size_ts=0
            };

            comma_count += _Value_ToDisk(
                val, comma_file, &line_counts, &comma_buffer
            );
        }

        if (c == DOUBLE_QUOTE) {
            quoted = !quoted;
        }

        else if (!quoted && c == COMMA) {
            size_t val = i + 1;
            comma_count +=
                _Value_ToDisk(val, comma_file, &line_counts, &comma_buffer);
            if (cols == SIZE_MAX || col_index < cols) {
                col_index += 1;
            } else {
                overflow_warning =
                    "column overflow encountered while parsing CSV, "
                    "extra values will be truncated!";
                overflow = i;
                for (;;) {
                  if (file[overflow] == LINE_FEED ||
                      file[overflow] == CARRIAGE_RETURN) {
                    break;
                  } else if (overflow >= file_len) {
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
            size_t val = i + 1;
            comma_count += _Value_ToDisk(
                val, comma_file, &line_counts, &comma_buffer
            );

            if (row_index == 0) {
                cols = col_index;
            }

            else if (col_index < cols) {
                underflow_warning =
                    "column underflow encountered while parsing CSV, "
                    "missing values will be filled with the empty bytestring!";
                while (col_index < cols) {
                    comma_count += _Value_ToDisk(
                        val, comma_file, &line_counts, &comma_buffer
                    );
                    col_index += 1;
                }
                if (comma_count > i)
                    goto underflow_error;
            }

            if (newline == -1) {
                newline = (c == CARRIAGE_RETURN && file[i + 1] == LINE_FEED)
                              ? LINE_FEED + CARRIAGE_RETURN
                              : c;
            }

            _BufferWrite(newline_file, &newline_buffer, &line_counts,
                         sizeof(LazyCSV_IndexCounts));

            col_index = 0;
            row_index += 1;
        }

        cm2 = cm1;
        cm1 = c;
    }

    char last_char = file[file_len - 1];

    char overcount = !!(last_char == CARRIAGE_RETURN || last_char == LINE_FEED);

    if (!overcount) {
        _BufferWrite(newline_file, &newline_buffer, &line_counts,
                     sizeof(LazyCSV_IndexCounts));
        _Value_ToDisk(file_len + 1, comma_file, &line_counts, &comma_buffer);
    }

    rows = row_index - overcount + skip_headers;
    cols = cols + 1;

    _BufferFlush(comma_file, &comma_buffer);
    _BufferFlush(newline_file, &newline_buffer);

    close(comma_file);
    close(newline_file);

    free(comma_buffer.data);
    free(newline_buffer.data);

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

    int comma_fd = open(comma_index, O_RDWR);
    struct stat comma_st;
    if (fstat(comma_fd, &comma_st) < 0) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "unable to stat index file"
        );
        return NULL;
    }

    int newline_fd = open(newline_index, O_RDWR);
    struct stat newline_st;
    if (fstat(newline_fd, &newline_st) < 0) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "unable to stat index file"
        );
        return NULL;
    }

    char *comma_memmap =
        mmap(NULL, comma_st.st_size, mmap_flags, MAP_PRIVATE, comma_fd, 0);
    char *newline_memmap =
        mmap(NULL, newline_st.st_size, mmap_flags, MAP_PRIVATE, newline_fd, 0);

    PyObject* headers;

    if (!skip_headers) {
        headers = PyTuple_New(cols);
        LazyCSV_IndexCounts* counts = (LazyCSV_IndexCounts*)newline_memmap;

        size_t cs, ce, len;
        char* addr;
        for (size_t i = 0; i < cols; i++) {
            cs = _Value_FromIndex(comma_memmap, i, counts);
            ce = _Value_FromIndex(comma_memmap, i+1, counts);

            if (ce - cs == 1) {
                PyTuple_SET_ITEM(headers, i, PyBytes_FromString(""));
            }
            else {
                addr = file + cs;
                len = ce - cs - 1;
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
    _cache->items = malloc(UCHAR_MAX*sizeof(PyObject*));

    for (unsigned char i = 0; i < UCHAR_MAX; i++)
        _cache->items[i] = PyBytes_FromString((char*)&i);

    LazyCSV_IndexFile* _commas = malloc(sizeof(LazyCSV_IndexFile));
    _commas->name = comma_index;
    _commas->data = comma_memmap;
    _commas->st = comma_st;
    _commas->fd = comma_fd;

    LazyCSV_IndexFile* _newlines = malloc(sizeof(LazyCSV_IndexFile));
    _newlines->name = newline_index;
    _newlines->data = newline_memmap;
    _newlines->st = newline_st;
    _newlines->fd = newline_fd;

    LazyCSV_Index* _index = malloc(sizeof(LazyCSV_Index));

    _index->dir = tempdir;
    _index->commas = _commas;
    _index->newlines = _newlines;

    LazyCSV_Data* _data = malloc(sizeof(LazyCSV_Data));
    _data->name = fullname;
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
    self->_newline = newline;
    self->_index = _index;
    self->_data = _data;
    self->_cache = _cache;

    return (PyObject*)self;

underflow_error:
    PyErr_SetString(
        PyExc_RuntimeError,
        "CSV parse in in unrecoverable_state due to a CSV row being"
        "considerably smaller than expected."
    );
    goto unrecoverable_state;

unrecoverable_state:

    munmap(file, ust.st_size);

    close(comma_file);
    close(newline_file);
    close(ufd);

    remove(comma_index);
    remove(newline_index);

    free(comma_buffer.data);
    free(newline_buffer.data);

    Py_XDECREF(tempdir);

    return NULL;
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
    munmap(self->_index->commas->data, self->_index->commas->st.st_size);
    munmap(self->_index->newlines->data, self->_index->newlines->st.st_size);

    close(self->_data->fd);
    close(self->_index->commas->fd);
    close(self->_index->newlines->fd);

    remove(self->_index->commas->name);
    remove(self->_index->newlines->name);

    free(self->_index->commas->name);
    free(self->_index->newlines->name);

    free(self->_index->commas);
    free(self->_index->newlines);

    Py_XDECREF(self->_index->dir);

    Py_DECREF(self->_cache->empty);
    for (size_t i = 0; i < UCHAR_MAX; i++)
        Py_DECREF(self->_cache->items[i]);
    free(self->_cache->items);

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

