#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include "pyobj.h"
#include <Functions/UserDefined/Proto.h>

class PyRunner {
public:
    struct ScalarArgsRunner {
        const std::string &mod;
        uint32_t batchMod;
        uint64_t version;
        PyObject *pOutput;
        PyObject *pInputs;
    };

    struct AggregateArgsRunner {
        const ScalarArgsRunner& scalar_args;
        PyObject *pStateMap;
    };

    PyRunner();

    template <typename T>
    int start(const T& arg);
    void setPyEntryName(const char *entry_name);

protected:
    bool isModUpdated(const std::string &fn, uint64_t version);

private:
    std::unordered_map<std::string, uint64_t> functionChecksum;
    PyObj entryNames[DB::Method::METHODS_CNT];
};
