#pragma once
#include <UDFIColumn.h>

#include "pyobj.h"
#include "udfstring.h"

class UDFColumnNullable;

class PyColumn {
public:
    PyColumn();
    PyObject* convert(UDFIColumn *column,
                      ssize_t rows,
                      std::vector<struct StrWriteBack> *wb);

private:
    PyObject *toMaskedColumn(UDFColumnNullable *c,
                            ssize_t rows,
                            std::vector<struct StrWriteBack> *wb);

    PyObject *toNdArray(UDFColumnNullable *c,
                        ssize_t rows,
                        std::vector<struct StrWriteBack> *wb);

    PyObject *CallNested(UDFColumnNullable *c,
                         ssize_t rows,
                         std::vector<struct StrWriteBack> *wb,
                         PyObject *pFunc,
                         PyObject **pRemainArgs,
                         size_t args);

private:
    PyObj MaskedArray;
    PyObj numpy;
    PyObj split;
};
