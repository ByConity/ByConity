#include "pyobj.h"
#include "pyerror.h"

static const char* getError(PyObject* obj, PyObject** pStr)
{
    PyObj repr(PyObject_Repr(obj));

    if (!repr)
        return "PyObject_Repr() fails";

    *pStr = PyUnicode_AsEncodedString(repr, "utf-8", "~E~");
    if (!*pStr)
        return "PyUnicode_AsEncodedString() fails";

    return PyBytes_AS_STRING(*pStr);
}

void handle_pyerror(std::function<void(const std::string &msg)> setError)
{
    PyObj pTrace;
    PyObj pType;
    PyObj pVal;
    PyObj pStr;

    PyErr_Fetch(&pType, &pVal, &pTrace);

    if (pVal)
        setError(getError(pVal, &pStr));
    else if (pType)
        setError(getError(pType, &pStr));

    // TODO: print callstack
    PyErr_Clear();
}
