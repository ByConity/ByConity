#include <ILanguageServer.h>
#include <UDFServer.h>
#include "pyerror.h"
#include "pyrunner.h"
#include "pycolumn.h"
#include <iostream>

class LangPython : public ILanguageServer
{
public:
    void calc(const ScalarArgsCalc & scalar_params) override {
        calcUDF<ScalarArgsCalc>(scalar_params);
    }

    void calc(const AggregateArgsCalc & aggregate_params) override {
        calcUDF<AggregateArgsCalc>(aggregate_params);
    }

private:
    template<typename T> void calcUDF(const T& arg);

    PyColumn column;
    PyRunner runner;
};

template<typename T>
void LangPython::calcUDF(const T& arg)
{

    const struct ScalarArgsCalc * scalar_args;
    std::vector<struct StrWriteBack> strs;
    PyObj pStateMap(nullptr);
    PyObj pOutput(nullptr);

    if constexpr (std::is_same_v<T, ScalarArgsCalc>)
        scalar_args = &arg;
    else
        scalar_args = &(arg.scalar_args);

    if constexpr (std::is_same_v<T, ScalarArgsCalc>)
        pOutput = column.convert(scalar_args->output, scalar_args->row_counts, &strs);
    else
        pOutput = column.convert(scalar_args->output, arg.state_counts, &strs);

    if (!pOutput) {
        scalar_args->setErr("Unsupported output type");
        handle_pyerror(scalar_args->setErr);
        return;
    }

    if constexpr (std::is_same_v<T, AggregateArgsCalc>) {
        if (scalar_args->batch_f_id & (DB::Method::ADD_BATCH | DB::Method::MERGE_DATA_BATCH)) {
            pStateMap = column.convert(arg.state_map, scalar_args->row_counts, nullptr);

            if (!pStateMap) {
                scalar_args->setErr("Unsupported output type");
                handle_pyerror(scalar_args->setErr);
                return;
            }
        }
    }

    PyObj pInputs(PyList_New(scalar_args->inputs.size()));

    if (!pInputs) {
        handle_pyerror(scalar_args->setErr);
        return;
    }

    for (size_t i = 0; i < scalar_args->inputs.size(); i++) {
        PyObject * pColumn(nullptr);

        if constexpr (std::is_same_v<T, AggregateArgsCalc>) {
            /*  For mergeDataBatch the size of first state column and second state column
                is different. In case of mergeDataBatch the first column i.e when i == 0,
                will have rows equal to no. of unique states. */
            if (i == 0 && scalar_args->batch_f_id == DB::Method::MERGE_DATA_BATCH)
                pColumn = column.convert(scalar_args->inputs[i].get(), arg.state_counts, nullptr);
            else
                pColumn = column.convert(scalar_args->inputs[i].get(), scalar_args->row_counts, nullptr);
        } else {
            pColumn = column.convert(scalar_args->inputs[i].get(), scalar_args->row_counts, nullptr);
        }

        if (!pColumn) {
            std::string err = "Unsupported input column ";

            scalar_args->setErr(err + std::to_string(i + 1));
            return;
        }

        PyList_SetItem(pInputs, i, pColumn);
    }

    const struct PyRunner::ScalarArgsRunner scalar_args_runner = {
        .mod = scalar_args->f_name,
        .batchMod = scalar_args->batch_f_id,
        .version = scalar_args->version,
        .pOutput = static_cast<PyObject *>(pOutput),
        .pInputs = static_cast<PyObject *>(pInputs),
    };

    int ret;
    if constexpr (std::is_same_v<T, AggregateArgsCalc>) {
        const struct PyRunner::AggregateArgsRunner agg_args_runner = {
            .scalar_args = scalar_args_runner,
            .pStateMap = static_cast<PyObject *>(pStateMap),
        };

        ret = runner.start(agg_args_runner);
    } else {
        ret = runner.start(scalar_args_runner);
    }

    if (ret) {
        handle_pyerror(scalar_args->setErr);
        return;
    }

    if (0 != OutString_WriteBack(strs)) {
        handle_pyerror(scalar_args->setErr);
        return;
    }
}

template void LangPython::calcUDF<LangPython::ScalarArgsCalc>(const ScalarArgsCalc& arg);
template void LangPython::calcUDF<LangPython::AggregateArgsCalc>(const AggregateArgsCalc& arg);

class PyRuntime {
public:
    PyRuntime() {
        Py_Initialize();
        if (PyErr_Occurred())
            throw(std::runtime_error("Py_Initialize failed"));
    }

    ~PyRuntime() {
        Py_Finalize();
    }
};

int main(int argc, char ** argv)
{
    PyRuntime rt;
    LangPython py;

    UDFServer server(&py);

    return server.run(argc, argv);
}
