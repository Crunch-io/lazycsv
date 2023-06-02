#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>

#include <Python.h>
#include "structmember.h"

#define LINE_FEED 10
#define CARRIAGE_RETURN 13

// users can set this macro using the env variable LAZYCSV_INDEX_DTYPE if you
// want to be more aggressive with minimizing index disk usage (i.e. define
// INDEX_DTYPE as uint8_t) but at a cost to performance.

#ifndef INDEX_DTYPE
#define INDEX_DTYPE uint16_t
#endif

#ifdef DEBUG
void PyDebug() {return;}
#endif

// optionally include a to_numpy() method on the iterable to materialize into a
// numpy array, requires numpy install and to be set explicitly using env
// variable LAZYCSV_INCLUDE_NUMPY=1, and LAZYCSV_INCLUDE_NUMPY_LEGACY=1 to
// install using legacy numpy APIs

#ifndef INCLUDE_NUMPY_LEGACY
#define INCLUDE_NUMPY_LEGACY 0
#endif
#if INCLUDE_NUMPY_LEGACY
#ifdef INCLUDE_NUMPY
#undef INCLUDE_NUMPY
#endif
#define INCLUDE_NUMPY 1
#else
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#endif
#ifndef INCLUDE_NUMPY
#define INCLUDE_NUMPY 0
#endif
#if INCLUDE_NUMPY
#include <numpy/arrayobject.h>
#endif


static size_t INDEX_DTYPE_MAX = ((INDEX_DTYPE) ~(INDEX_DTYPE)0);


typedef struct {
    char* data;
    size_t size;
    size_t capacity;
} LazyCSV_Buffer;


typedef struct {
    PyObject* empty;
    PyObject** items;
} LazyCSV_Cache;


typedef struct {
    size_t col;
    size_t value;
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
    char _quotechar;
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
    size_t stop;
    size_t step;
    char reversed;
} LazyCSV_Iter;


static inline void LazyCSV_BufferWrite(int fd, LazyCSV_Buffer *buffer,
                                       void *data, size_t size) {

    if (buffer->size + size >= buffer->capacity) {
        write(fd, buffer->data, buffer->size);
        buffer->size = 0;
    }
    memcpy(&buffer->data[buffer->size], data, size);
    buffer->size += size;
}


static inline void LazyCSV_BufferCache(LazyCSV_Buffer *buffer, void *data,
                                       size_t size) {

    if (size == 0) return;

    if (buffer->size + size >= buffer->capacity) {
        buffer->capacity *= 1.5;
        buffer->data = realloc(buffer->data, buffer->capacity);
    }
    memcpy(&buffer->data[buffer->size], data, size);
    buffer->size += size;
}


static inline void LazyCSV_BufferFlush(int comma_file, LazyCSV_Buffer *buffer) {
    write(comma_file, buffer->data, buffer->size);
    buffer->size = 0;
    fsync(comma_file);
}


static inline void LazyCSV_ValueToDisk(size_t value, LazyCSV_RowIndex *ridx,
                                       LazyCSV_AnchorPoint *apnt,
                                       size_t col_index, int cfile,
                                       LazyCSV_Buffer *cbuf, int afile,
                                       LazyCSV_Buffer *abuf) {

    size_t target = value - apnt->value;

    if (target > INDEX_DTYPE_MAX) {
        *apnt = (LazyCSV_AnchorPoint){.value = value, .col = col_index+1};
        LazyCSV_BufferWrite(afile, abuf, apnt, sizeof(LazyCSV_AnchorPoint));
        ridx->count += 1;
        target = 0;
    }

    INDEX_DTYPE item = target;

    LazyCSV_BufferWrite(cfile, cbuf, &item, sizeof(INDEX_DTYPE));
}


static inline size_t LazyCSV_AnchorValueFromValue(size_t value,
                                                  LazyCSV_AnchorPoint *amap,
                                                  LazyCSV_RowIndex *ridx) {

    LazyCSV_AnchorPoint *apnt = amap + ridx->count - 1;

    if (value >= apnt->col) {
        // we hit this if there is only one anchor point, or we're iterating
        // over the last anchor point.
        return apnt->value;
    }

    LazyCSV_AnchorPoint* apntp1;
    size_t L = 0, R = ridx->count-1;

    while (L <= R) {
        size_t M = (L + R) / 2;
        apnt = amap + M;
        apntp1 = apnt + 1;
        if (value > apntp1->col) {
            L = M + 1;
        }
        else if (value < apnt->col) {
            R = M - 1;
        }
        else if (value == apntp1->col) {
            return apntp1->value;
        }
        else {
            return apnt->value;
        }
    }
    return SIZE_MAX;
}


static inline size_t LazyCSV_ValueFromIndex(size_t value,
                                            LazyCSV_RowIndex *ridx, char *cmap,
                                            char *amap) {

    size_t cval = *(INDEX_DTYPE *)(cmap + (value * sizeof(INDEX_DTYPE)));
    size_t aval =
        LazyCSV_AnchorValueFromValue(value, (LazyCSV_AnchorPoint *)amap, ridx);
    return aval == SIZE_MAX ? aval : cval + aval;
}


static inline void LazyCSV_IterCol(LazyCSV_Iter *iter, size_t *offset,
                                   size_t *len) {

    LazyCSV *lazy = (LazyCSV *)iter->lazy;

    if (iter->position < iter->stop) {
        size_t position =
            iter->reversed
                ? lazy->rows - 1 - iter->position + !lazy->_skip_headers
                : iter->position + !lazy->_skip_headers;

        iter->position += iter->step;

        char* newlines = lazy->_index->newlines->data;
        char* anchors = lazy->_index->anchors->data;
        char* commas = lazy->_index->commas->data;

        LazyCSV_RowIndex* ridx =
            (LazyCSV_RowIndex*)
            (newlines + position*sizeof(LazyCSV_RowIndex));

        char* aidx = anchors+ridx->index;
        char* cidx = commas+((lazy->cols+1)*position*sizeof(INDEX_DTYPE));

        size_t cs = LazyCSV_ValueFromIndex(iter->col, ridx, cidx, aidx);
        size_t ce = LazyCSV_ValueFromIndex(iter->col + 1, ridx, cidx, aidx);

        *len = ce - cs - 1;
        *offset = cs;
    }
}


static inline void LazyCSV_IterRow(LazyCSV_Iter *iter, size_t *offset,
                                   size_t *len) {

    LazyCSV *lazy = (LazyCSV *)iter->lazy;

    if (iter->position < iter->stop) {
        size_t position =
            iter->reversed ? lazy->cols - iter->position - 1 : iter->position;

        iter->position += iter->step;

        char* newlines = lazy->_index->newlines->data;
        char* anchors = lazy->_index->anchors->data;
        char* commas = lazy->_index->commas->data;

        size_t row = iter->row + !lazy->_skip_headers;

        LazyCSV_RowIndex* ridx =
            (LazyCSV_RowIndex*)
            (newlines + row*sizeof(LazyCSV_RowIndex));

        char *aidx = anchors + ridx->index;
        char *cidx = commas + ((lazy->cols + 1) * row * sizeof(INDEX_DTYPE));

        size_t cs = LazyCSV_ValueFromIndex(position, ridx, cidx, aidx);
        size_t ce = LazyCSV_ValueFromIndex(position + 1, ridx, cidx, aidx);

        *len = ce - cs - 1;
        *offset = cs;
    }
}


static inline PyObject *PyBytes_FromOffsetAndLen(LazyCSV *lazy, size_t offset,
                                                 size_t len) {

    PyObject* result;
    char* addr;

    switch (len) {
    case SIZE_MAX:
    case 0:
        // short circuit if result is empty string
        result = lazy->_cache->empty;
        Py_INCREF(result);
        break;
    case 1:
        addr = lazy->_data->data + offset;
        result = lazy->_cache->items[(size_t)*addr];
        Py_INCREF(result);
        break;
    default:
        addr = lazy->_data->data + offset;

        char strip_quotes = (
            lazy->_unquote
            && addr[0] == lazy->_quotechar
            && addr[len-1] == lazy->_quotechar
        );

        if (strip_quotes) {
            addr = addr+1;
            len = len-2;
        }

        result = PyBytes_FromStringAndSize(addr, len);
    }

    return result;
}


static PyObject* LazyCSV_IterNext(PyObject* self) {
    LazyCSV_Iter* iter = (LazyCSV_Iter*)self;
    LazyCSV *lazy = (LazyCSV *)iter->lazy;

    size_t offset = SIZE_MAX, len;

    switch ((iter->row == SIZE_MAX) - (iter->col == SIZE_MAX)) {
    case -1:
        LazyCSV_IterRow(iter, &offset, &len);
        break;
    case +1:
        LazyCSV_IterCol(iter, &offset, &len);
        break;
    default:
        PyErr_SetString(
            PyExc_RuntimeError,
            "could not determine axis for materialization"
        );
        return NULL;
    }

    if (offset==SIZE_MAX) {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    return PyBytes_FromOffsetAndLen(lazy, offset, len);
}


static PyObject* LazyCSV_IterAsList(PyObject* self) {
    LazyCSV_Iter* iter = (LazyCSV_Iter*)self;
    LazyCSV* lazy = (LazyCSV*)iter->lazy;

    size_t size;
    size_t iter_col = iter->col;
    size_t iter_row = iter->row;

    if (iter_col == SIZE_MAX) {
        size = lazy->cols - iter->position;
    }
    else if (iter_row == SIZE_MAX) {
        size = lazy->rows - iter->position;
    }
    else {
        PyErr_SetString(
            PyExc_RuntimeError,
            "could not determine axis for materialization"
        );
        return NULL;
    }

    PyObject* result = PyList_New(size);
    size_t offset=SIZE_MAX, len=0;

    PyObject* item;
    for (size_t i = 0; i < size; i++) {
        switch (iter_col) {
        case SIZE_MAX:
            LazyCSV_IterRow(iter, &offset, &len);
            break;
        default:
            LazyCSV_IterCol(iter, &offset, &len);
        }
        item = PyBytes_FromOffsetAndLen(lazy, offset, len);
        PyList_SET_ITEM(result, i, item);
    }

    return result;
}


#if INCLUDE_NUMPY
static PyObject* LazyCSV_IterAsNumpy(PyObject* self) {
    LazyCSV_Iter* iter = (LazyCSV_Iter*)self;
    LazyCSV* lazy = (LazyCSV*)iter->lazy;

    size_t size;
    size_t iter_col = iter->col;
    size_t iter_row = iter->row;

    if (iter_col != SIZE_MAX) {
        size = lazy->rows - iter->position;
    }
    else if (iter_row != SIZE_MAX) {
        size = lazy->cols - iter->position;
    }
    else {
        PyErr_SetString(
            PyExc_RuntimeError,
            "could not determine axis for materialization"
        );
        return NULL;
    }

    size_t buffer_capacity = 65536; // 2**16
    LazyCSV_Buffer buffer = {.data = malloc(buffer_capacity),
                             .size = 0,
                             .capacity = buffer_capacity};

    size_t offset, len=0, max_len=0;
    char* addr;

    for (size_t i=0; i < size; i++) {
        switch (iter_col) {
        case SIZE_MAX:
            LazyCSV_IterRow(iter, &offset, &len);
            break;
        default:
            LazyCSV_IterCol(iter, &offset, &len);
        }
        addr = lazy->_data->data + offset;
        LazyCSV_BufferCache(&buffer, &len, sizeof(size_t));
        LazyCSV_BufferCache(&buffer, addr, len);
        max_len = len > max_len ? len : max_len;
    }

    npy_intp const dimensions[1] = {size, };
    npy_intp const strides[1] = {max_len, };

    PyArrayObject *arr =
        (PyArrayObject *)PyArray_New(&PyArray_Type, 1, dimensions, NPY_STRING,
                                     strides, NULL, max_len, 0, NULL);

    if (!arr) {
        free(buffer.data);
        PyErr_SetString(
            PyExc_RuntimeError,
            "could not allocate numpy array"
        );
        return NULL;
    }

    char* tempbuf = buffer.data;
    char* arrdata = PyArray_DATA(arr);

    for (size_t i = 0; i < size; i++) {
        len = *(size_t *)tempbuf;
        tempbuf += sizeof(size_t);
        size_t padlen = max_len - len;
        strncpy(arrdata, tempbuf, len);
        tempbuf += len;
        arrdata += len;
        memset(arrdata, 0, padlen);
        arrdata += padlen;
    }

    free(buffer.data);

    return PyArray_Return(arr);
}
#endif


static PyObject* LazyCSV_IterSelf(PyObject* self) {
    Py_INCREF(self);
    return self;
}


static void LazyCSV_IterDestruct(LazyCSV_Iter* self) {
    Py_DECREF(self->lazy);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyMethodDef LazyCSV_IterMethods[] = {
#if INCLUDE_NUMPY
    {
        "to_numpy",
        (PyCFunction)LazyCSV_IterAsNumpy,
        METH_NOARGS,
        "materialize iterator into a numpy array"
    },
#endif
    {
        "to_list",
        (PyCFunction)LazyCSV_IterAsList,
        METH_NOARGS,
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


static inline void LazyCSV_TempDirAsString(PyObject **tempdir, char **dirname) {
    PyObject *tempfile = PyImport_ImportModule("tempfile");
    PyObject *tempdir_obj =
        PyObject_GetAttrString(tempfile, "TemporaryDirectory");

    *tempdir = PyObject_CallObject(tempdir_obj, NULL);
    PyObject* dirname_obj = PyObject_GetAttrString(*tempdir, "name");
    PyObject* dirstring = PyUnicode_AsUTF8String(dirname_obj);
    *dirname = PyBytes_AsString(dirstring);

    Py_DECREF(tempfile);
    Py_DECREF(tempdir_obj);
    Py_DECREF(dirname_obj);
    Py_DECREF(dirstring);
}


static inline void LazyCSV_FullNameFromName(PyObject *name,
                                            PyObject **fullname_obj,
                                            char **fullname) {

    PyObject *os_path = PyImport_ImportModule("os.path");
    PyObject *isfile = PyObject_CallMethod(os_path, "isfile", "O", name);

    PyObject *builtins = PyImport_ImportModule("builtins");
    PyObject* global_vars = PyObject_CallMethod(builtins, "globals", NULL);

    // borrowed ref
    PyObject* __file__ = PyDict_GetItemString(global_vars, "__file__");

    if (isfile == Py_True) {
        // owned reference which we keep
        *fullname_obj = PyObject_CallMethod(os_path, "abspath", "O", name);
        *fullname = PyBytes_AsString(*fullname_obj);
    }

    else if (__file__) {
        // also check to see if file is relative to the caller if not
        // previously found
        PyObject *_dirname =
            PyObject_CallMethod(os_path, "dirname", "O", __file__);

        PyObject *dirname = PyUnicode_AsUTF8String(_dirname);

        PyObject *joined =
            PyObject_CallMethod(os_path, "join", "(OO)", dirname, name);

        *fullname_obj = PyObject_CallMethod(os_path, "abspath", "O", joined);
        *fullname = PyBytes_AsString(*fullname_obj);

        Py_DECREF(joined);
        Py_DECREF(_dirname);
        Py_DECREF(dirname);
    }

    Py_DECREF(os_path);
    Py_DECREF(isfile);
    Py_DECREF(builtins);
    Py_DECREF(global_vars);
}


static PyObject *LazyCSV_New(PyTypeObject *type, PyObject *args,
                             PyObject *kwargs) {

    PyObject* name;
    int skip_headers = 0;
    int unquote = 1;
    Py_ssize_t buffer_capacity = 2097152; // 2**21
    char *dirname, *delimiter = ",", *quotechar = "\"";

    static char* kwlist[] = {
        "", "delimiter", "quotechar", "skip_headers", "unquote", "buffer_size", "index_dir", NULL
    };

    char ok = PyArg_ParseTupleAndKeywords(
        args, kwargs, "O|ssppns", kwlist, &name, &delimiter, &quotechar,
        &skip_headers, &unquote, &buffer_capacity, &dirname);

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
    LazyCSV_FullNameFromName(name, &fullname_obj, &fullname);

    Py_DECREF(name);

    int ufd = open(fullname, O_RDONLY);
    if (ufd == -1) {
        PyErr_SetString(
            PyExc_FileNotFoundError,
            "unable to open data file,"
            " check to be sure that the user has read permissions"
            " and/or ownership of the file, and that the file exists."
        );
        goto return_err;
    }

    struct stat ust;
    if (fstat(ufd, &ust) < 0) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "unable to stat user file"
        );
        goto close_ufd;
    }

    size_t file_len = ust.st_size;

    int mmap_flags = PROT_READ;
    char* file = mmap(NULL, file_len, mmap_flags, MAP_PRIVATE, ufd, 0);

    PyObject* tempdir = NULL;
    if (!dirname) {
        LazyCSV_TempDirAsString(&tempdir, &dirname);
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

    LazyCSV_Buffer comma_buffer = {.data = malloc(buffer_capacity),
                                   .size = 0,
                                   .capacity = buffer_capacity};

    LazyCSV_Buffer anchor_buffer = {.data = malloc(buffer_capacity),
                                    .size = 0,
                                    .capacity = buffer_capacity};

    LazyCSV_Buffer newline_buffer = {.data = malloc(buffer_capacity),
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

            LazyCSV_BufferWrite(anchor_file, &anchor_buffer, &apnt,
                                sizeof(LazyCSV_AnchorPoint));

            ridx.index += ridx.count*sizeof(LazyCSV_AnchorPoint);
            ridx.count = 1;

            LazyCSV_ValueToDisk(val, &ridx, &apnt, col_index, comma_file,
                                &comma_buffer, anchor_file, &anchor_buffer);
        }

        if (c == *quotechar) {
            quoted = !quoted;
        }

        else if (!quoted && c == *delimiter) {
            size_t val = i + 1;
            LazyCSV_ValueToDisk(val, &ridx, &apnt, col_index, comma_file,
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
                LazyCSV_ValueToDisk(val, &ridx, &apnt, col_index, comma_file,
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
                  LazyCSV_ValueToDisk(val, &ridx, &apnt, col_index, comma_file,
                                      &comma_buffer, anchor_file,
                                      &anchor_buffer);
                  col_index += 1;
                }
            }

            if (newline == -1) {
                newline = (c == CARRIAGE_RETURN && file[i + 1] == LINE_FEED)
                              ? LINE_FEED + CARRIAGE_RETURN
                              : c;
            }

            LazyCSV_BufferWrite(newline_file, &newline_buffer, &ridx,
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
        LazyCSV_ValueToDisk(file_len + 1, &ridx, &apnt, col_index, comma_file,
                            &comma_buffer, anchor_file, &anchor_buffer);

        LazyCSV_BufferWrite(newline_file, &newline_buffer, &ridx,
                            sizeof(LazyCSV_RowIndex));
    }

    if (overflow_warning)
        PyErr_WarnEx(
            PyExc_RuntimeWarning,
            overflow_warning,
            1
        );

    if (underflow_warning)
        PyErr_WarnEx(
            PyExc_RuntimeWarning,
            underflow_warning,
            1
        );

    rows = row_index - overcount + skip_headers;
    cols = cols + 1;

    LazyCSV_BufferFlush(comma_file, &comma_buffer);
    LazyCSV_BufferFlush(anchor_file, &anchor_buffer);
    LazyCSV_BufferFlush(newline_file, &newline_buffer);

    close(comma_file);
    close(anchor_file);
    close(newline_file);

    free(comma_buffer.data);
    free(anchor_buffer.data);
    free(newline_buffer.data);

    int comma_fd = open(comma_index, O_RDWR);
    struct stat comma_st;
    if (fstat(comma_fd, &comma_st) < 0) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "unable to stat comma file"
        );
        goto close_comma;
    }

    int anchor_fd = open(anchor_index, O_RDWR);
    struct stat anchor_st;
    if (fstat(anchor_fd, &anchor_st) < 0) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "unable to stat anchor file"
        );
        goto close_anchor;
    }

    int newline_fd = open(newline_index, O_RDWR);
    struct stat newline_st;
    if (fstat(newline_fd, &newline_st) < 0) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "unable to stat newline file"
        );
        goto close_newline;
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
        char *addr;
        for (size_t i = 0; i < cols; i++) {
            cs = LazyCSV_ValueFromIndex(i, ridx, comma_memmap, anchor_memmap);
            ce = LazyCSV_ValueFromIndex(i + 1, ridx, comma_memmap,
                                        anchor_memmap);

            if (ce - cs == 1) {
                PyTuple_SET_ITEM(headers, i, PyBytes_FromString(""));
            }
            else {
                addr = file + cs;

                len = ce - cs - 1;
                if (unquote
                        && addr[0] == *quotechar
                        && addr[len-1] == *quotechar) {
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
        goto unmap_memmaps;
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
    self->_quotechar = *quotechar;
    self->_newline = newline;
    self->_index = _index;
    self->_data = _data;
    self->_cache = _cache;

    return (PyObject*)self;

unmap_memmaps:
    munmap(comma_memmap, comma_st.st_size);
    munmap(anchor_memmap, anchor_st.st_size);
    munmap(newline_memmap, newline_st.st_size);
    Py_DECREF(headers);
    goto close_newline;

close_newline:
    close(newline_fd);
    goto close_anchor;

close_anchor:
    close(anchor_fd);
    goto close_comma;

close_comma:
    close(comma_fd);
    munmap(file, ust.st_size);
    Py_XDECREF(tempdir);
    goto close_ufd;

close_ufd:
    close(ufd);
    goto return_err;

return_err:
    return NULL;
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


static PyObject *LazyCSV_Seq(PyObject *self, PyObject *args, PyObject *kwargs) {

    size_t row = SIZE_MAX;
    size_t col = SIZE_MAX;
    size_t stop;
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

    if (col != SIZE_MAX) {
        stop = ((LazyCSV*)self)->rows;
    }
    else if (row != SIZE_MAX) {
        stop = ((LazyCSV*)self)->cols;
    }
    else {
        PyErr_SetString(
            PyExc_RuntimeError,
            "could not determine axis for materialization"
        );
        return NULL;
    }

    PyTypeObject* type = &LazyCSV_IterType;
    LazyCSV_Iter* iter = (LazyCSV_Iter*)type->tp_alloc(type, 0);

    if (!iter) {
        PyErr_SetString(
            PyExc_MemoryError,
            "unable to allocate memory for iterable"
        );
        return NULL;
    }

    iter->row = row;
    iter->col = col;
    iter->reversed = reversed;
    iter->position = 0;
    iter->step = 1;
    iter->stop = stop;
    iter->lazy = self;

    Py_INCREF(self);

    return (PyObject*)iter;
}


static PyObject* LazyCSV_GetValue(PyObject* self, PyObject* r, PyObject* c) {

    Py_ssize_t _row = PyLong_AsSsize_t(r);
    Py_ssize_t _col = PyLong_AsSsize_t(c);

    LazyCSV* lazy = (LazyCSV*)self;

    size_t row = _row < 0 ? lazy->rows + _row : (size_t)_row;
    size_t col = _col < 0 ? lazy->cols + _col : (size_t)_col;

    int row_in_bounds = (
        0 <= row && row < lazy->rows
    );

    int col_in_bounds = (
        0 <= col && col < lazy->cols
    );

    if (!row_in_bounds || !col_in_bounds) {
        PyErr_SetString(
            PyExc_ValueError,
            "provided value not in bounds of index"
        );
        return NULL;
    }

    row += !lazy->_skip_headers;

    char* newlines = lazy->_index->newlines->data;
    char* anchors = lazy->_index->anchors->data;
    char* commas = lazy->_index->commas->data;

    LazyCSV_RowIndex* ridx =
        (LazyCSV_RowIndex*)
        (newlines + row*sizeof(LazyCSV_RowIndex));

    char* aidx = anchors+ridx->index;
    char* cidx = commas+((lazy->cols+1)*row*sizeof(INDEX_DTYPE));

    size_t cs = LazyCSV_ValueFromIndex((size_t)col, ridx, cidx, aidx);
    size_t ce = LazyCSV_ValueFromIndex((size_t)col + 1, ridx, cidx, aidx);

    size_t len = ce - cs - 1;

    return PyBytes_FromOffsetAndLen(lazy, cs, len);
}


static PyObject* LazyCSV_GetItem(PyObject* self, PyObject* key) {
    if (!PyTuple_Check(key)) {
        PyErr_SetString(
            PyExc_ValueError,
            "index must contain both a row and column value"
        );
        return NULL;
    }

    PyObject *row_obj, *col_obj;

    if (!PyArg_ParseTuple(key, "OO", &row_obj, &col_obj)) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "unable to parse index key"
        );
        return NULL;
    }

    if (PyLong_Check(row_obj) && PyLong_Check(col_obj))
        return LazyCSV_GetValue(self, row_obj, col_obj);

    int row_is_slice = PySlice_Check(row_obj);
    int col_is_slice = PySlice_Check(col_obj);

    LazyCSV* lazy = (LazyCSV*)self;

    if (row_is_slice && !col_is_slice) {
        PySliceObject* row_slice = (PySliceObject*)row_obj;

        Py_ssize_t _col = PyLong_AsSsize_t(col_obj);
        size_t col = _col < 0 ? lazy->cols + _col : (size_t)_col;

        int col_in_bounds = (
            0 <= col && col < lazy->cols
        );

        if (!col_in_bounds) goto boundary_err;

        Py_ssize_t _start = row_slice->start == Py_None
                                ? (Py_ssize_t)0
                                : PyLong_AsSsize_t(row_slice->start);
        Py_ssize_t _stop = row_slice->stop == Py_None
                               ? (Py_ssize_t)lazy->rows
                               : PyLong_AsSsize_t(row_slice->stop);
        Py_ssize_t _step =
            row_slice->step == Py_None ? 1 : PyLong_AsSsize_t(row_slice->step);

        size_t start = _start < 0 ? lazy->rows + _start : (size_t)_start;
        size_t stop = _stop < 0 ? lazy->rows + _stop : (size_t)_stop;

        size_t step;
        char reversed = 0;

        if (_step < 0) {
            reversed = 1;
            step = (size_t)(-1 * _step);
            if (row_slice->start != Py_None) {
                start = lazy->rows - start - 1;
            }
            if (row_slice->stop != Py_None) {
                stop = lazy->rows - stop - 1;
            }
        }
        else {
            step = (size_t)_step;
        }

        PyTypeObject* type = &LazyCSV_IterType;
        LazyCSV_Iter* iter = (LazyCSV_Iter*)type->tp_alloc(type, 0);
        if (!iter) goto memory_err;

        iter->row = SIZE_MAX;
        iter->col = col;
        iter->reversed = reversed;
        iter->position = start;
        iter->step = step;
        iter->stop = stop;
        iter->lazy = self;
        Py_INCREF(self);

        return (PyObject*)iter;
    }

    if (col_is_slice && !row_is_slice) {
        PySliceObject* col_slice = (PySliceObject*)col_obj;

        Py_ssize_t _row = PyLong_AsSsize_t(row_obj);
        size_t row = _row < 0 ? lazy->rows + _row : (size_t)_row;

        int row_in_bounds = (
            0 <= row && row < lazy->rows
        );

        if (!row_in_bounds) goto boundary_err;

        Py_ssize_t _start = col_slice->start == Py_None
                                ? (Py_ssize_t)0
                                : PyLong_AsSsize_t(col_slice->start);
        Py_ssize_t _stop = col_slice->stop == Py_None
                               ? (Py_ssize_t)lazy->cols
                               : PyLong_AsSsize_t(col_slice->stop);
        Py_ssize_t _step =
            col_slice->step == Py_None ? 1 : PyLong_AsSsize_t(col_slice->step);

        size_t start = _start < 0 ? lazy->cols + _start : (size_t)_start;
        size_t stop = _stop < 0 ? lazy->cols + _stop : (size_t)_stop;

        size_t step;
        char reversed = 0;

        if (_step < 0) {
            step = (size_t)(-1 * _step);
            reversed = 1;
            if (col_slice->start != Py_None) {
                start = lazy->cols - start - 1;
            }
            if (col_slice->stop != Py_None) {
                stop = lazy->cols - stop - 1;
            }
        }
        else {
            step = (size_t)_step;
        }

        PyTypeObject* type = &LazyCSV_IterType;
        LazyCSV_Iter* iter = (LazyCSV_Iter*)type->tp_alloc(type, 0);
        if (!iter) goto memory_err;

        iter->row = row;
        iter->col = SIZE_MAX;
        iter->reversed = reversed;
        iter->position = start;
        iter->step = step;
        iter->stop = stop;
        iter->lazy = self;
        Py_INCREF(self);

        return (PyObject*)iter;
    }

    goto schema_err;

schema_err:
    PyErr_SetString(
        PyExc_ValueError,
        "given indexing schema is not supported"
    );
    return NULL;

memory_err:
    PyErr_SetString(
        PyExc_MemoryError,
        "unable to allocate memory for iterable"
    );
    return NULL;

boundary_err:
    PyErr_SetString(
        PyExc_ValueError,
        "provided value not in bounds of index"
    );
    return NULL;
}


static PyMemberDef LazyCSV_Members[] = {
    {"headers", T_OBJECT, offsetof(LazyCSV, headers), READONLY, "header tuple"},
    {"rows", T_LONG, offsetof(LazyCSV, rows), READONLY, "row length"},
    {"cols", T_LONG, offsetof(LazyCSV, cols), READONLY, "col length"},
    {"name", T_OBJECT, offsetof(LazyCSV, name), READONLY, "file name"},
    {NULL, }
};


static PyMethodDef LazyCSV_Methods[] = {
    {
        "sequence",
        (PyCFunction)LazyCSV_Seq,
        METH_VARARGS|METH_KEYWORDS,
        "get column iterator"
    },
    {NULL, }
};

static PyMappingMethods LazyCSV_MappingMembers[] = {
    (lenfunc)NULL,
    (binaryfunc)LazyCSV_GetItem,
    (objobjargproc)NULL,
};

PyDoc_STRVAR(
    LazyCSV_Docstring,
    "lazycsv.LazyCSV(\n"
    "    filepath,\n"
    "    /\n"
    "    unquoted: bool=True,\n"
    "    skip_headers: bool=False,\n"
    "    buffer_size: int=2**21,\n"
    "    index_dir: str=None,\n"
    ")\n"
    "\n"
    "LazyCSV object constructor. Takes the filepath of a CSV\n"
    "file as the first argument, and several keyword arguments\n"
    "as optional values. Indexes the CSV, generates headers,\n"
    "and returns `self` to the caller."
    "\n\n"
    "Options\n"
    "-------\n"
    "unquoted: bool=True -- if True, a quoted field will be\n"
    "    stripped of quotes on parsing. i.e. `,\"goo\\nbar\",`\n"
    "    will return 'goo\\nbar'.\n"
    "skip_headers: bool=False -- skips parsing out header\n"
    "    values to the .header attribute if True.\n"
    "buffer_size: int=2**21 -- is the buffer size that LazyCSV\n"
    "    uses when writing index data to disk during object\n"
    "    construction, can be set to any value greater than 0\n"
    "    (units of bytes).\n"
    "index_dir: str=None -- Directory where index files\n"
    "    are saved. By default uses Python's `TemporaryDirectory()`\n"
    "    function in the `tempfile` module.\n"
    "\n"
    "Returns\n"
    "-------\n"
    "self\n"
);


static PyTypeObject LazyCSVType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "lazycsv.LazyCSV",
    .tp_doc = LazyCSV_Docstring,
    .tp_basicsize = sizeof(LazyCSV),
    .tp_dealloc = (destructor)LazyCSV_Destruct,
    .tp_flags = Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,
    .tp_methods = LazyCSV_Methods,
    .tp_members = LazyCSV_Members,
    .tp_as_mapping = LazyCSV_MappingMembers,
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
#if INCLUDE_NUMPY
    import_array();
#endif
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

