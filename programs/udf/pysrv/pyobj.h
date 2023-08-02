#pragma once
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <object.h>
#include <structmember.h>

class PyObj {
    PyObject *p;

public:
    PyObj(PyObject *ptr = nullptr) {
        p = ptr;
    }

    ~PyObj() {
        Py_XDECREF(p);
    }

    operator PyObject *() {
        return p;
    }

    bool operator!() {
        return !p;
    }

    PyObject **operator &() {
        return &p;
    }

    PyObj &operator=(PyObject *ptr) {
        Py_XDECREF(p);
        p = ptr;
        return *this;
    }
};
