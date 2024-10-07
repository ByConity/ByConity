#pragma once

#include <Common/Logger.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Interpreters/Set.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

// TODO: Yuanning RuntimeFilter; Replace with older version
class RuntimeFilterBloomFilterExists : public IFunction
{
public:
    static constexpr auto name = "runtimeFilterBloomFilterExist";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<RuntimeFilterBloomFilterExists>(std::move(context)); }

    explicit RuntimeFilterBloomFilterExists(ContextPtr context_)
        : context(std::move(context_)), log(getLogger("RuntimeFilterBloomFilterExists"))
    {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!checkAndGetDataType<DataTypeString>(arguments[0].get()))
            throw Exception("Function " + getName() + " first argument string is required", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

    bool prepare(const ColumnsWithTypeAndName & arguments) const
    {
        std::lock_guard lock(mu);
        if (ready)
            return true;

        if (!dynamic_value)
        {
            dynamic_value_key = arguments[0].column->getDataAt(0).toString();
            dynamic_value = RuntimeFilterManager::getInstance().getDynamicValue(dynamic_value_key);
        }

        bool is_ready = dynamic_value->isReady();
        if (!is_ready)
        {
            total_count++;
            if (total_count > 128) // no need anymore
            {
                ready = true;
                bypass = BypassType::BYPASS_LARGE_HT;
            }
            return false;
        }

        auto const & value = dynamic_value->get(0);
        if (value.bypass != BypassType::NO_BYPASS)
        {
            bypass = value.bypass;
            ready = true;
            return true;
        }

        if (value.is_local)
        {
            auto const & d = std::get<RuntimeFilterVal>(value.data);
            if (!d.is_bf)
            {
                ready = true;
                bypass = BypassType::BYPASS_LARGE_HT;
                return true;
            }
            else
            {
                bloom_filter = d.bloom_filter;
                LOG_DEBUG(log, "probe worker try get dynamic value, key: {}, value: {}", dynamic_value_key, bloom_filter->debugString());
                total_count = 0;
                ready = true;
                return true;
            }
        }
        else
        {
            auto const & d = std::get<InternalDynamicData>(value.data);
            if (d.bf.isNull())
            {
                ready = true;
                bypass = BypassType::BYPASS_LARGE_HT;
                return true;
            }
            const auto & data = d.bf.get<String>();
            ReadBufferFromMemory read_buffer(data.data(), data.size());
            bloom_filter = std::make_shared<BloomFilterWithRange>();
            bloom_filter->deserialize(read_buffer);
            LOG_DEBUG(log, "probe worker try get dynamic value, key: {}, value: {}", dynamic_value_key, bloom_filter->debugString());
            total_count = 0;
            ready = true;
            return true;
        }
    }

private:
    ContextPtr context;
    LoggerPtr log;

    mutable std::mutex mu;
    mutable String dynamic_value_key;
    mutable DynamicValuePtr dynamic_value;
    mutable BloomFilterWithRangePtr bloom_filter;
    mutable BypassType bypass = BypassType::NO_BYPASS;
    mutable bool ready{false};
    mutable std::atomic<UInt32> eval_count {0};
    mutable std::atomic<UInt32> total_count {0};
};

}
