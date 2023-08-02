#include <stdexcept>
#include "pyrunner.h"

static const char *getClassName(const std::string &fn)
{
    size_t pos = fn.find_last_of('.');

    if (pos == std::string::npos)
        return fn.c_str();

    return fn.c_str() + pos + 1;
}

PyRunner::PyRunner()
{
    const char* methods[DB::METHODS_CNT] = {
        "entry",
        "addBatch",
        "addBatchSinglePlace",
        "mergeBatch",
        "mergeDataBatch"
    };

    for (size_t i = 0; i < DB::METHODS_CNT; ++i)
        entryNames[i] = PyUnicode_FromString(methods[i]);
}

bool PyRunner::isModUpdated(const std::string &fn, uint64_t version)
{
    const auto &it = functionChecksum.find(fn);

    if (it == functionChecksum.end()) {
        functionChecksum[fn] = version;
        return false;
    }

    if (it->second == version)
        return false;

    functionChecksum[fn] = version;
    return true;
}

template <typename T>
int PyRunner::start(const T &arg)
{
    const struct ScalarArgsRunner * scalar_args;

    if constexpr (std::is_same_v<T, ScalarArgsRunner>)
        scalar_args = &arg;
    else
        scalar_args = &arg.scalar_args;

    PyObj pName(PyUnicode_DecodeFSDefault(scalar_args->mod.c_str()));
    if (!pName)
        return -1;

    PyObj pModule(PyImport_Import(pName));
    if (!pModule)
        return -1;

    if (isModUpdated(scalar_args->mod, scalar_args->version)) {
        pModule = PyImport_ReloadModule(pModule);

        if (!pModule)
            return -1;
    }

    PyObj pClass(PyObject_GetAttrString(pModule, getClassName(scalar_args->mod)));
    if (!pClass)
        return -1;

    PyObj pInst(PyObject_CallObject(pClass, nullptr));
    if (!pInst)
        return -1;

    PyObject * pyEntryName(entryNames[__builtin_ctz(scalar_args->batchMod)]);

    PyObj pFn(PyObject_GetAttr(pClass, pyEntryName));
    if (!pFn || !PyCallable_Check(pFn))
        return -1;

    PyObj pRet(nullptr);

    if constexpr (std::is_same_v<T, ScalarArgsRunner>)
        pRet = PyObject_CallFunctionObjArgs(pFn,
                                            static_cast<PyObject *>(pInst),
                                            static_cast<PyObject *>(scalar_args->pOutput),
                                            static_cast<PyObject *>(scalar_args->pInputs),
                                            nullptr);
    else
        pRet = PyObject_CallFunctionObjArgs(pFn,
                                            static_cast<PyObject *>(pInst),
                                            static_cast<PyObject *>(scalar_args->pOutput),
                                            static_cast<PyObject *>(scalar_args->pInputs),
                                            static_cast<PyObject *>(arg.pStateMap),
                                            nullptr);

    return !pRet;
}

template int PyRunner::start<PyRunner::ScalarArgsRunner>(const ScalarArgsRunner &arg);
template int PyRunner::start<PyRunner::AggregateArgsRunner>(const AggregateArgsRunner &arg);
