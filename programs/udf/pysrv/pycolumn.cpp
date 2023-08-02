#include <cstdio>
#include <string>

#include "pycolumn.h"
#include "pyerror.h"
#include "udfstring.h"

#include <UDFColumnString.h>
#include <UDFColumnFixedString.h>
#include <UDFColumnArray.h>
#include <UDFColumnNullable.h>
#include <UDFColumnDecimal.h>

#define PY_ARRAY_UNIQUE_SYMBOL pysrv_ARRAY_API
#define NPY_NO_DEPRECATED_API NPY_API_VERSION
#include <numpy/npy_common.h>
#include <numpy/arrayobject.h>
#include <numpy/ufuncobject.h>
#include <numpy/npy_3kcompat.h>
#include <numpy/ndarraytypes.h>

#define APPLY_FOR_EACH_PYNUM(V) \
    V(UInt8   , NPY_UINT8  ) \
    V(UInt16  , NPY_UINT16 ) \
    V(UInt32  , NPY_UINT32 ) \
    V(UInt64  , NPY_UINT64 ) \
    V(Int8    , NPY_INT8   ) \
    V(Int16   , NPY_INT16  ) \
    V(Int32   , NPY_INT32  ) \
    V(Int64   , NPY_INT64  ) \
    V(Float32 , NPY_FLOAT32) \
    V(Float64 , NPY_FLOAT64) \
    V(Date    , NPY_UINT16 ) \
    V(DateTime, NPY_UINT32 )

void printError(const std::string &msg)
{
    printf("%s\n", msg.c_str());
}

static PyObject *getMaskedArray(PyObject *np)
{
    PyObj ma(PyObject_GetAttrString(np, "ma"));

    if (!ma)
        return nullptr;

    return PyObject_GetAttrString(ma, "MaskedArray");
}

PyColumn::PyColumn()
{
    PyObj numpy_str(PyUnicode_FromString("numpy"));
    if (!numpy_str)
        throw(std::runtime_error("failed to create numpy str"));

    numpy = PyImport_Import(numpy_str);
    if (!numpy) {
        handle_pyerror(printError);
        throw(std::runtime_error("failed to import numpy"));
    }

    if (_import_array() < 0) {
        handle_pyerror(printError);
        throw(std::runtime_error("failed to import array"));
    }

    MaskedArray = getMaskedArray(numpy);
    if (!MaskedArray) {
        handle_pyerror(printError);
        throw(std::runtime_error("failed to import MaskedArray"));
    }

    split = PyObject_GetAttrString(numpy, "split");
    if (!split) {
        handle_pyerror(printError);
        throw(std::runtime_error("failed to get numpy.split()"));
    }
}

PyObject *PyColumn::CallNested(UDFColumnNullable *c,
                               ssize_t rows,
                               std::vector<struct StrWriteBack> *wb,
                               PyObject *pFunc,
                               PyObject **pRemainArgs,
                               size_t args)
{
    PyObject *pData = nullptr;
    PyObject *pArgs = nullptr;
    PyObject *pRet = nullptr;

    pData = convert(c->getNestedColumn(), rows, wb);
    if (!pData)
        goto _Exit;

    pArgs = PyTuple_New(1 + args);
    if (!pArgs)
        goto _Exit;

    PyTuple_SetItem(pArgs, 0, pData);
    for (size_t i = 0; i < args; i++)
        PyTuple_SetItem(pArgs, i + 1, pRemainArgs[0]);

    pRet = PyObject_CallObject(pFunc, pArgs);

_Exit:
    if (!pRet) {
        if (pArgs) {
            Py_DECREF(pArgs);
        } else {
            Py_XDECREF(pData);
            for (size_t i = 0; i < args; i++)
                Py_XDECREF(pRemainArgs[i]);
        }
    }
    return pRet;
}

PyObject *PyColumn::toMaskedColumn(UDFColumnNullable *c,
                                   ssize_t rows,
                                   std::vector<struct StrWriteBack> *wb)
{
    PyObject *pMask;

    pMask = PyArray_SimpleNewFromData(1, &rows, NPY_BOOL, c->getData());
    if (!pMask)
        return nullptr;

    return CallNested(c, rows, wb, MaskedArray, &pMask, 1);
}

PyObject *PyColumn::toNdArray(UDFColumnNullable *c,
                              ssize_t rows,
                              std::vector<struct StrWriteBack> *wb)
{
    uint64_t *ptr = static_cast<uint64_t*>(c->getData());
    PyObject *pOffset;

    rows -= 1;
    pOffset = PyArray_SimpleNewFromData(1, &rows, NPY_UINT64, ptr);
    if (!pOffset)
        return nullptr;

    return CallNested(c, ptr[rows], wb, split, &pOffset, 1);
}

PyObject *PyColumn::convert(UDFIColumn *col,
                            ssize_t rows,
                            std::vector<struct StrWriteBack> *wb)
{
    int flags = wb ? NPY_ARRAY_WRITEABLE : 0;
    DB::TypeIndex type = col->type;
    void *data = col->getData();

    switch(type) {
#define V(IDX, TYPE) \
    case DB::TypeIndex::IDX: \
        return PyArray_SimpleNewFromData(1, &rows, (TYPE), data);
    APPLY_FOR_EACH_PYNUM(V)
#undef V

#define ARRNEW(TYPE, SZ) \
    PyArray_New(&PyArray_Type, 1, &rows, (TYPE), NULL, data, (SZ), flags, NULL)

    case DB::TypeIndex::UUID:
        return ARRNEW(NPY_STRING, 16);

    case DB::TypeIndex::String: {
        const UDFColumnString *c = static_cast<const UDFColumnString *>(col);

        if (!wb)
            return InString_FROM(c, rows);

        PyObject *out = OutString_Alloc(rows);

        if (!out)
            return nullptr;

        wb->emplace_back(StrWriteBack{const_cast<UDFColumnString*>(c), out});
        return out;
    }

    case DB::TypeIndex::FixedString: {
        UDFColumnFixedString *c = static_cast<UDFColumnFixedString *>(col);

        return ARRNEW(NPY_STRING, c->getStringLength());
    }

    case DB::TypeIndex::Nullable:
        return toMaskedColumn(static_cast<UDFColumnNullable *>(col), rows, wb);

    case DB::TypeIndex::Decimal32:
    case DB::TypeIndex::Decimal64:
        // TODO: subclassing
        // UDFColumnDecimal<DB::Decimal64>

        /* rule for scale
        add, subtract: S = max(S1, S2).
        multuply: S = S1 + S2.
        divide: S = S1.
        */
        break;
    case DB::TypeIndex::Array:
        return toNdArray(static_cast<UDFColumnNullable *>(col), rows, wb);
    case DB::TypeIndex::UInt128:
    case DB::TypeIndex::Int128:
    case DB::TypeIndex::Function:
    case DB::TypeIndex::AggregateFunction:
    case DB::TypeIndex::LowCardinality:
    case DB::TypeIndex::Map:
    case DB::TypeIndex::BitMap64:
    case DB::TypeIndex::Decimal128:
    case DB::TypeIndex::Tuple:
    case DB::TypeIndex::Set:
    case DB::TypeIndex::Interval:
    case DB::TypeIndex::Enum8:
    case DB::TypeIndex::Enum16:
    default:
        break;
    }
    return nullptr;
}
