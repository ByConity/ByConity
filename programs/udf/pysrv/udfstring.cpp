#include <functional>
#include "udfstring.h"

#define NO_IMPORT_ARRAY
#define PY_ARRAY_UNIQUE_SYMBOL pysrv_ARRAY_API
#define NPY_NO_DEPRECATED_API NPY_API_VERSION
#include <numpy/npy_common.h>
#include <numpy/arrayobject.h>
#include <numpy/ufuncobject.h>
#include <numpy/npy_3kcompat.h>
#include <numpy/ndarraytypes.h>

struct WriteBackState {
    UDFColumnString *c;
    uint64_t *offset;
    char *data;    /* current chars pointer */
    size_t curr;   /* current offset, including left padding */
    size_t len;    /* length of the mmap */
};

static PyArray_Descr *descr;

typedef struct NewNpyArrayIterObject_tag {
    PyObject_HEAD
    /* The iterator */
    void *iter;
    /* Flag indicating iteration started/stopped */
    char started, finished;
    /* Child to update for nested iteration */
    void *nested_child;
    /* Cached values from the iterator */
    void *iternext;
    void *get_multi_index;
    char **dataptrs;
    void **dtypes;
    PyArrayObject **operands;
} NewNpyArrayIterObject;

static char *get_base(void *obj)
{
    PyArrayObject *ref = static_cast<PyArrayObject *>(obj);
    PyObject *base = PyArray_BASE(ref);

    if (base) {
        NewNpyArrayIterObject *it = reinterpret_cast<NewNpyArrayIterObject *>(base);

        ref = *it->operands;
    }

    return static_cast<char *>(PyArray_DATA(ref));
}

static PyObject* getitem(void* data, void* arr)
{
    const char *base = get_base(arr);
    const UDFColumnString *col;
    const uint64_t *offset;
    const char *str;
    Py_ssize_t len;
    size_t i;

    i = static_cast<const char *>(data) - base;
    col = reinterpret_cast<const UDFColumnString *>(base);
    offset = col->getOffset();

    if (!i) {
        len = offset[i];
        str = static_cast<const char *>(col->getChars());
    } else {
        len = offset[i] - offset[i - 1];
        str = static_cast<const char *>(col->getChars()) + offset[i - 1];
    }

    if (len && !str[len - 1])
        len--;

    return Py_BuildValue("s#", str, len);
}

void UDFString_Deinit()
{
    if (!descr)
        return;

    free(descr);
    descr = nullptr;
}

int UDFString_Init(void)
{
    const PyArray_Descr *pd = PyArray_DescrFromType(NPY_STRING);
    static PyArray_ArrFuncs f;

    if (!pd) {
        fprintf(stderr, "failed to get string descr\n");
        return -1;
    }

    f.getitem = getitem;
    descr = static_cast<PyArray_Descr *>(malloc(sizeof(*descr)));
    if (!descr)
        return -1;

    memcpy(descr, pd, sizeof(*descr));
    descr->flags = NPY_NEEDS_PYAPI | NPY_USE_GETITEM;
    descr->f = &f;
    descr->elsize = 1;
    return 0;
}

PyObject *InString_FROM(const UDFColumnString *col, ssize_t sz)
{
#ifdef BUILD_PYARRAY_STRING
    if (!col)
        return NULL;

    return PyArray_NewFromDescr(&PyArray_Type, descr, 1, &sz, 0,
                                const_cast<UDFColumnString *>(col),
                                0, NULL);
#else
    /* build tuple directly */
    char *data = static_cast<char *>(col->getChars());
    uint64_t *offset = col->getOffset();
    PyObject *obj = PyTuple_New(sz);
    int last_offset = 0;

	if (!obj)
		return nullptr;

    for (int i = 0; i < sz; i++) {
        const char *str = data + last_offset;
        size_t len = *offset - last_offset;

        last_offset = *offset++;

        if (len && !str[len - 1])
            len--;

        PyTuple_SetItem(obj, i, Py_BuildValue("s#", str, len));
    }

    return obj;
#endif
}

PyObject *OutString_Alloc(size_t sz)
{
    npy_intp dims = sz;

    return PyArray_EMPTY(1, &dims, NPY_OBJECT, 0);
}

static int foreach_array(PyObject *arr, std::function<int(PyObject *)> fn)
{
    NpyIter_IterNextFunc *iternext;
    npy_intp* innersizeptr;
    npy_intp* strideptr;
    char** dataptr;
    NpyIter* iter;
    int ret = -1;

    iter = NpyIter_New(reinterpret_cast<PyArrayObject *>(arr),
                       NPY_ITER_READONLY |
                       NPY_ITER_EXTERNAL_LOOP |
                       NPY_ITER_REFS_OK,
                       NPY_KEEPORDER, NPY_NO_CASTING, nullptr);
    if (!iter)
        return -1;

    iternext = NpyIter_GetIterNext(iter, nullptr);
    if (!iternext)
        goto Exit;

    innersizeptr = NpyIter_GetInnerLoopSizePtr(iter);
    strideptr = NpyIter_GetInnerStrideArray(iter);
    dataptr = NpyIter_GetDataPtrArray(iter);

    do {
        /* Get the inner loop data/stride/count values */
        npy_intp count = *innersizeptr;
        npy_intp stride = *strideptr;
        char* data = *dataptr;

        /* This is a typical inner loop for NPY_ITER_EXTERNAL_LOOP */
        while (count--) {
            if (fn(*reinterpret_cast<PyObject **>(data)))
                goto Exit;
            data += stride;
        }

        /* Increment the iterator to the next inner loop */
    } while(iternext(iter));

    ret = 0;

Exit:
    NpyIter_Deallocate(iter);
    return ret;
}

int OutString_WriteBack(std::vector<struct StrWriteBack> &strs)
{
    for (auto &wb : strs) {
        struct WriteBackState state = {
            .c = wb.col,
            .offset = wb.col->getOffset(),
            .data = wb.col->getChars(),
            .curr = 0,
            .len = wb.col->getLength(),
        };

        int ret = foreach_array(wb.arr, [&state](PyObject *obj) {
            const size_t padding = 16;
            size_t zero_termed_len;
            size_t expected;
            Py_ssize_t sz;
            char *buf;

            if (PyUnicode_Check(obj)) {
                buf = static_cast<char *>(PyUnicode_DATA(obj));
                sz = PyUnicode_GET_LENGTH(obj);
            } else {
                if (-1 == PyString_AsStringAndSize(obj, &buf, &sz))
                    return -1;
            }

            zero_termed_len = sz + 1;

            /* enlarge mmap area if running out of buffer */
            expected = state.curr + zero_termed_len + padding;
            if (__builtin_expect(expected > state.len, 0)) {
                state.len = state.c->enlarge(expected);
                state.data = state.c->getChars() + state.curr;
            }

            *state.offset = state.curr + zero_termed_len;
            state.curr = *state.offset++;

            memcpy(state.data, buf, sz);

            state.data[sz] = 0;
            state.data += zero_termed_len;

            return 0;
        });

        if (__builtin_expect(ret, 0))
            return ret;

        wb.col->setCharsLength(state.curr);
    }

    return 0;
}
