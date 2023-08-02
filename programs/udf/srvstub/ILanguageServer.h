#pragma once
#include <memory>
#include <string>
#include <vector>
#include <cstdint>

#include "UDFIColumn.h"

class ILanguageServer
{
    public:
        struct ScalarArgsCalc
        {
            const std::string & f_name;
            uint32_t batch_f_id;
            std::vector<std::unique_ptr<UDFIColumn>> &inputs;
            UDFIColumn *output;
            size_t row_counts;
            std::function<void(const std::string &msg)> setErr;
            const uint64_t version;
        };

        struct AggregateArgsCalc
        {
            const ScalarArgsCalc & scalar_args;
            UDFIColumn * state_map;
            size_t state_counts;
        };

        virtual ~ILanguageServer() = default;

        virtual void calc(const ScalarArgsCalc & scalar_params) = 0;
        virtual void calc(const AggregateArgsCalc & aggregate_params) = 0;
};
