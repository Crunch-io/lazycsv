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
    size_t value;
    size_t col;
} LazyCSV_AnchorPoint;


typedef struct {
    size_t index;
    size_t count;
} LazyCSV_RowIndex;


typedef struct {
    int fd;
    struct stat st;
    char* name;
    char* data;
} LazyCSV_File;


typedef struct {
    PyObject* dir;
    LazyCSV_File* commas;
    LazyCSV_File* anchors;
    LazyCSV_File* newlines;
} LazyCSV_Index;


typedef struct {
    PyObject_HEAD
    PyObject* headers;
    PyObject* name;
    size_t rows;
    size_t cols;
    int _skip_headers;
    int _unquote;
    char _newline;
    LazyCSV_Index* _index;
    LazyCSV_File* _data;
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


static inline void _BufferWrite(
    int fd, WriteBuffer* buffer, void* data, size_t size
) {
    if (buffer->size + size >= buffer->capacity) {
        write(fd, buffer->data, buffer->size);
        buffer->size = 0;
    }
    memcpy(&buffer->data[buffer->size], data, size);
    buffer->size += size;
}


static inline void _BufferFlush(int comma_file, WriteBuffer* buffer) {
    write(comma_file, buffer->data, buffer->size);
    buffer->size = 0;
    fsync(comma_file);
}


static inline void _Value_ToDisk(
    size_t value, LazyCSV_RowIndex *ridx, LazyCSV_AnchorPoint *apnt,
    size_t col_index, int cfile, WriteBuffer *cbuf,
    int afile, WriteBuffer *abuf
) {

    Py_ssize_t target = value - apnt->value;

    if (target > UCHAR_MAX) {
        *apnt = (LazyCSV_AnchorPoint){.value = value, .col = col_index+1};
        _BufferWrite(afile, abuf, apnt, sizeof(LazyCSV_AnchorPoint));
        ridx->count += 1;
        target = 0;
    }

    unsigned char item = target;

    _BufferWrite(cfile, cbuf, &item, sizeof(char));
}


static inline size_t _AnchorValue_FromValue(size_t value, char *amap,
                                     LazyCSV_RowIndex *ridx) {

    LazyCSV_AnchorPoint apnt, apntp1;

    apnt = *(LazyCSV_AnchorPoint *)(amap + ((ridx->count - 1) *
                                            sizeof(LazyCSV_AnchorPoint)));
    if (value >= apnt.col) {
        return apnt.value;
    }

    size_t L = 0, R = ridx->count-1;

    size_t asize = sizeof(LazyCSV_AnchorPoint);

    while (L <= R) {
        size_t M = (L + R) / 2;
        apnt = *(LazyCSV_AnchorPoint *)(amap + (asize * M));
        apntp1 = *(LazyCSV_AnchorPoint *)(amap + (asize * (M + 1)));
        if (value > apntp1.col) {
            L = M + 1;
        }
        else if (value < apnt.col) {
            R = M - 1;
        }
        else if (value == apntp1.col) {
            return apntp1.value;
        }
        else {
            // apnt.col <= value && value < apntp1.col
            return apnt.value;
        }
    }
    return SIZE_MAX;
}


static inline size_t _Value_FromIndex(size_t value, LazyCSV_RowIndex *ridx,
                                      char *cmap, char *amap) {

    size_t cval = *(unsigned char*)(cmap+(value*sizeof(char)));
    size_t aval = _AnchorValue_FromValue(value, amap, ridx);
    return cval+aval;
}


static inline PyObject* LazyCSV_IterCol(LazyCSV_Iter* iter) {
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
        char* anchors = lazy->_index->anchors->data;
        char* commas = lazy->_index->commas->data;

        LazyCSV_RowIndex* ridx =
            (LazyCSV_RowIndex*)
            (newlines + position*sizeof(LazyCSV_RowIndex));

        char* aidx = anchors+ridx->index;
        char* cidx = commas+((lazy->cols+1)*position);

        size_t cs = _Value_FromIndex(iter->col, ridx, cidx, aidx);
        size_t ce = _Value_FromIndex(iter->col + 1, ridx, cidx, aidx);

        size_t len = ce - cs - 1;
        char* addr;

        switch (len) {
        case SIZE_MAX:
        case 0:
            // short circuit if result is empty string
            result = lazy->_cache->empty;
            Py_INCREF(result);
            break;
        case 1:
            addr = lazy->_data->data + cs;
            result = lazy->_cache->items[(size_t)*addr];
            Py_INCREF(result);
            break;
        default:
            addr = lazy->_data->data + cs;

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


static inline PyObject* LazyCSV_IterRow(LazyCSV_Iter* iter) {
    LazyCSV* lazy = (LazyCSV*)iter->lazy;

    PyObject* result = NULL;

    if (iter->position < lazy->cols) {
        size_t position = iter->reversed ?
            lazy->cols - iter->position - 1 : iter->position;

        iter->position += 1;

        char* newlines = lazy->_index->newlines->data;
        char* anchors = lazy->_index->anchors->data;
        char* commas = lazy->_index->commas->data;

        size_t row = iter->row + !lazy->_skip_headers;

        LazyCSV_RowIndex* ridx =
            (LazyCSV_RowIndex*)
            (newlines + row*sizeof(LazyCSV_RowIndex));

        char* aidx = anchors+ridx->index;
        char* cidx = commas+((lazy->cols+1)*row);

        Py_ssize_t cs = _Value_FromIndex(position, ridx, cidx, aidx);
        Py_ssize_t ce = _Value_FromIndex(position + 1, ridx, cidx, aidx);

        size_t len = ce - cs - 1;

        char* addr;

        switch (len) {
        case SIZE_MAX:
        case 0:
            // short circuit if result is empty string
            result = lazy->_cache->empty;
            Py_INCREF(result);
        case 1:
            addr = lazy->_data->data + cs;
            result = lazy->_cache->items[(size_t)*addr];
            Py_INCREF(result);
        default:
            addr = lazy->_data->data + cs;

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


static inline void _TempDir_AsString(PyObject** tempdir, char** dirname) {
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

static inline void _FullName_FromName(PyObject *name, PyObject **fullname_obj,
                                      char **fullname) {

    PyObject *os_path = PyImport_ImportModule("os.path");
    PyObject *builtins = PyImport_ImportModule("builtins");

    PyObject* global_vars = PyObject_CallMethod(builtins, "globals", NULL);

    PyObject* _dirname;

    // borrowed ref
    PyObject* __file__ = PyDict_GetItemString(global_vars, "__file__");

    if (!__file__) {
        // if run in say, the python shell, there is no __file__, so use cwd
        // of the interpreter
        PyObject *os = PyImport_ImportModule("os");
        _dirname = PyObject_CallMethod(os, "getcwd", NULL);
        Py_DECREF(os);
    }

    else {
        _dirname = PyObject_CallMethod(os_path, "dirname", "O", __file__);
    }

    PyObject *dirname = PyUnicode_AsUTF8String(_dirname);
    Py_DECREF(_dirname);

    PyObject *abspath = PyObject_CallMethod(os_path, "abspath", "O", name);

    PyObject *bytes = PyObject_GetAttrString(builtins, "bytes");
    PyObject *startswith =
        PyObject_CallMethod(bytes, "startswith", "OO", abspath, dirname);

    if (startswith == Py_True) {
        *fullname_obj = abspath;
        Py_INCREF(abspath);
        *fullname = PyBytes_AsString(abspath);
    }

    else {
        PyObject *joined =
            PyObject_CallMethod(os_path, "join", "(OO)", dirname, name);
        *fullname_obj = PyObject_CallMethod(os_path, "abspath", "O", joined);
        *fullname = PyBytes_AsString(*fullname_obj);
        Py_DECREF(joined);
    }

    Py_DECREF(os_path);
    Py_DECREF(builtins);
    Py_DECREF(global_vars);
    Py_DECREF(dirname);
    Py_DECREF(abspath);
    Py_DECREF(bytes);
    Py_DECREF(startswith);
}

static PyObject* LazyCSV_New(
        PyTypeObject* type, PyObject* args, PyObject* kwargs
) {

    PyObject* name;
    int skip_headers = 0;
    int unquote = 1;
    Py_ssize_t buffer_capacity = 2097152; // 2**21
    char* dirname;

    static char* kwlist[] = {
        "", "skip_headers", "unquote", "buffer_size", "index_dir", NULL
    };

    char ok = PyArg_ParseTupleAndKeywords(
        args, kwargs, "O|ppns", kwlist, &name,
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

    Py_INCREF(name);
    if (PyUnicode_CheckExact(name)) {
        PyObject* _name = PyUnicode_AsUTF8String(name);
        Py_DECREF(name);
        name = _name;
    }

    if (!PyBytes_CheckExact(name)) {
        PyErr_SetString(
            PyExc_ValueError,
            "first argument must be str or bytes"
        );
        Py_DECREF(name);
        return NULL;
    }

    PyObject* fullname_obj;
    char* fullname;
    _FullName_FromName(name, &fullname_obj, &fullname);

    Py_DECREF(name);

    int ufd = open(fullname, O_RDONLY);
    if (ufd == -1) {
        PyErr_SetString(
            PyExc_FileNotFoundError,
            "unable to open data file,"
            " check to be sure that the user has read permissions"
            " and/or ownership of the file, and that the file exists."
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

    char* comma_index = tempnam(dirname, "LzyC_");
    char* anchor_index = tempnam(dirname, "LzyA_");
    char* newline_index = tempnam(dirname, "LzyN_");

    int file_flags = O_WRONLY|O_CREAT|O_EXCL;

    int comma_file = open(comma_index, file_flags, S_IRWXU);
    int anchor_file = open(anchor_index, file_flags, S_IRWXU);
    int newline_file = open(newline_index, file_flags, S_IRWXU);

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

    LazyCSV_RowIndex ridx = {.index = 0, .count = 0};

    LazyCSV_AnchorPoint apnt;

    WriteBuffer comma_buffer = {.data = malloc(buffer_capacity),
                                .size = 0,
                                .capacity = buffer_capacity};

    WriteBuffer anchor_buffer = {.data = malloc(buffer_capacity),
                                 .size = 0,
                                 .capacity = buffer_capacity};

    WriteBuffer newline_buffer = {.data = malloc(buffer_capacity),
                                  .size = 0,
                                  .capacity = buffer_capacity};

    for (size_t i = 0; i < file_len; i++) {

        if (overflow != SIZE_MAX && i < overflow) {
            continue;
        }

        c = file[i];

        if (col_index == 0
            && (cm1 == LINE_FEED || cm1 == CARRIAGE_RETURN)
            && cm2 != CARRIAGE_RETURN) {
            size_t val = (
                newline == (CARRIAGE_RETURN+LINE_FEED)
            ) ? i + 1 : i;

            apnt = (LazyCSV_AnchorPoint){.value = val, .col = col_index};

            _BufferWrite(anchor_file, &anchor_buffer, &apnt,
                         sizeof(LazyCSV_AnchorPoint));

            ridx.index += ridx.count*sizeof(LazyCSV_AnchorPoint);
            ridx.count = 1;

            _Value_ToDisk(val, &ridx, &apnt, col_index, comma_file,
                          &comma_buffer, anchor_file, &anchor_buffer);
        }

        if (c == DOUBLE_QUOTE) {
            quoted = !quoted;
        }

        else if (!quoted && c == COMMA) {
            size_t val = i + 1;
            _Value_ToDisk(val, &ridx, &apnt, col_index, comma_file,
                          &comma_buffer, anchor_file, &anchor_buffer);
            if (cols == SIZE_MAX || col_index < cols) {
                col_index += 1;
            }
            else {
                overflow_warning =
                    "column overflow encountered while parsing CSV, "
                    "extra values will be truncated!";
                overflow = i;

                for (;;) {
                    if (file[overflow] == LINE_FEED ||
                        file[overflow] == CARRIAGE_RETURN)
                        break;
                    else if (overflow >= file_len)
                        break;
                  overflow += 1;
                }
            }
        }

        else if (!quoted && c == LINE_FEED && cm1 == CARRIAGE_RETURN) {
            // no-op, don't match next block for \r\n
        }

        else if (!quoted && (c == CARRIAGE_RETURN || c == LINE_FEED)) {
            size_t val = i + 1;

            if (overflow == SIZE_MAX) {
                _Value_ToDisk(val, &ridx, &apnt, col_index, comma_file,
                              &comma_buffer, anchor_file, &anchor_buffer);
            }
            else {
                overflow = SIZE_MAX;
            }

            if (row_index == 0) {
                cols = col_index;
            }

            else if (col_index < cols) {
                underflow_warning =
                    "column underflow encountered while parsing CSV, "
                    "missing values will be filled with the empty bytestring!";
                while (col_index < cols) {
                    _Value_ToDisk(val, &ridx, &apnt, col_index, comma_file,
                                  &comma_buffer, anchor_file, &anchor_buffer);
                    col_index += 1;
                }
            }

            if (newline == -1) {
                newline = (c == CARRIAGE_RETURN && file[i + 1] == LINE_FEED)
                              ? LINE_FEED + CARRIAGE_RETURN
                              : c;
            }

            _BufferWrite(newline_file, &newline_buffer, &ridx,
                         sizeof(LazyCSV_RowIndex));

            col_index = 0;
            row_index += 1;
        }

        cm2 = cm1;
        cm1 = c;
    }

    char last_char = file[file_len - 1];
    char overcount = last_char == CARRIAGE_RETURN || last_char == LINE_FEED;

    if (!overcount) {
        _Value_ToDisk(file_len + 1, &ridx, &apnt, col_index, comma_file,
                      &comma_buffer, anchor_file, &anchor_buffer);

        _BufferWrite(newline_file, &newline_buffer, &ridx,
                     sizeof(LazyCSV_RowIndex));
    }

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

    rows = row_index - overcount + skip_headers;
    cols = cols + 1;

    _BufferFlush(comma_file, &comma_buffer);
    _BufferFlush(anchor_file, &anchor_buffer);
    _BufferFlush(newline_file, &newline_buffer);

    close(comma_file);
    close(anchor_file);
    close(newline_file);

    free(comma_buffer.data);
    free(anchor_buffer.data);
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
            "unable to stat comma file"
        );
        return NULL;
    }

    int anchor_fd = open(anchor_index, O_RDWR);
    struct stat anchor_st;
    if (fstat(anchor_fd, &anchor_st) < 0) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "unable to stat anchor file"
        );
        return NULL;
    }

    int newline_fd = open(newline_index, O_RDWR);
    struct stat newline_st;
    if (fstat(newline_fd, &newline_st) < 0) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "unable to stat newline file"
        );
        return NULL;
    }

    char *comma_memmap =
        mmap(NULL, comma_st.st_size, mmap_flags, MAP_PRIVATE, comma_fd, 0);
    char *anchor_memmap =
        mmap(NULL, anchor_st.st_size, mmap_flags, MAP_PRIVATE, anchor_fd, 0);
    char *newline_memmap =
        mmap(NULL, newline_st.st_size, mmap_flags, MAP_PRIVATE, newline_fd, 0);

    PyObject* headers;

    if (!skip_headers) {
        headers = PyTuple_New(cols);
        LazyCSV_RowIndex* ridx = (LazyCSV_RowIndex*)newline_memmap;

        size_t cs, ce;
        size_t len;
        char* addr;
        for (size_t i = 0; i < cols; i++) {
            cs = _Value_FromIndex(i, ridx, comma_memmap, anchor_memmap);
            ce = _Value_FromIndex(i+1, ridx, comma_memmap, anchor_memmap);

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

    LazyCSV* self = (LazyCSV*)type->tp_alloc(type, 0);
    if (!self) {
        PyErr_SetString(
            PyExc_MemoryError,
            "unable to allocate LazyCSV object"
        );
        Py_DECREF(headers);
        Py_XDECREF(tempdir);
        return NULL;
    }

    LazyCSV_Cache* _cache = malloc(sizeof(LazyCSV_Cache));
    _cache->empty = PyBytes_FromString("");
    _cache->items = malloc(UCHAR_MAX*sizeof(PyObject*));

    for (size_t i = 0; i < UCHAR_MAX; i++)
        _cache->items[i] = PyBytes_FromFormat("%c", (int)i);

    LazyCSV_File* _commas = malloc(sizeof(LazyCSV_File));
    _commas->name = comma_index;
    _commas->data = comma_memmap;
    _commas->st = comma_st;
    _commas->fd = comma_fd;

    LazyCSV_File* _anchors = malloc(sizeof(LazyCSV_File));
    _anchors->name = anchor_index;
    _anchors->data = anchor_memmap;
    _anchors->st = anchor_st;
    _anchors->fd = anchor_fd;

    LazyCSV_File* _newlines = malloc(sizeof(LazyCSV_File));
    _newlines->name = newline_index;
    _newlines->data = newline_memmap;
    _newlines->st = newline_st;
    _newlines->fd = newline_fd;

    LazyCSV_Index* _index = malloc(sizeof(LazyCSV_Index));

    _index->dir = tempdir;
    _index->commas = _commas;
    _index->newlines = _newlines;
    _index->anchors = _anchors;

    LazyCSV_File* _data = malloc(sizeof(LazyCSV_File));
    _data->name = fullname;
    _data->fd = ufd;
    _data->data = file;
    _data->st = ust;

    self->rows = rows;
    self->cols = cols;
    self->name = fullname_obj;
    self->headers = headers;
    self->_skip_headers = skip_headers;
    self->_unquote = unquote;
    self->_newline = newline;
    self->_index = _index;
    self->_data = _data;
    self->_cache = _cache;

    return (PyObject*)self;
}


static void LazyCSV_Destruct(LazyCSV* self) {
    munmap(self->_data->data, self->_data->st.st_size);
    munmap(self->_index->commas->data, self->_index->commas->st.st_size);
    munmap(self->_index->anchors->data, self->_index->anchors->st.st_size);
    munmap(self->_index->newlines->data, self->_index->newlines->st.st_size);

    close(self->_data->fd);
    close(self->_index->commas->fd);
    close(self->_index->anchors->fd);
    close(self->_index->newlines->fd);

    remove(self->_index->commas->name);
    remove(self->_index->anchors->name);
    remove(self->_index->newlines->name);

    free(self->_index->commas->name);
    free(self->_index->anchors->name);
    free(self->_index->newlines->name);

    free(self->_index->commas);
    free(self->_index->anchors);
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
    Py_DECREF(self->name);

    Py_TYPE(self)->tp_free((PyObject*)self);
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


static PyMemberDef LazyCSVMembers[] = {
    {"headers", T_OBJECT, offsetof(LazyCSV, headers), READONLY, "header tuple"},
    {"rows", T_LONG, offsetof(LazyCSV, rows), READONLY, "row length"},
    {"cols", T_LONG, offsetof(LazyCSV, cols), READONLY, "col length"},
    {"name", T_OBJECT, offsetof(LazyCSV, name), READONLY, "file name"},
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

