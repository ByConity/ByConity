#pragma once
#include <vector>
#include <UDFColumnString.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <object.h>
#include <structmember.h>

struct StrWriteBack {
    UDFColumnString *col;
    PyObject *arr;
};

PyObject *InString_FROM(const UDFColumnString *col, ssize_t sz);
PyObject *OutString_Alloc(size_t sz);
int OutString_WriteBack(std::vector<struct StrWriteBack> &strs);
