#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFunnelPathSplit.h>
#include <AggregateFunctions/AggregateFunctionFunnelPathSplitByTimes.h>
#include <DataTypes/DataTypeNullable.h>

#include <common/constexpr_helpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

template <typename TYPE>
std::vector<TYPE> transformArrayIntoVector(const Array & array) 
{
    std::vector<TYPE> res;
    for (Field field : array)
        res.push_back(field.get<TYPE>());
    return res;
}

template <bool is_terminating_event = false, bool deduplicate = false, bool is_by_times = false>
AggregateFunctionPtr createAggregateFunctionFunnelPathSplit(const String & name, const DataTypes & argument_types, const Array & params, const Settings * )
{
    if (params.size() != 4)
        throw Exception("Aggregate function " + name + " requires 4 parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() < 3)
        throw Exception(
            "Aggregate function " + name + " requires at least 3 arguments, but parsed " + std::to_string(argument_types.size()),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    UInt64 window = params[0].safeGet<UInt64>();
    UInt64 max_session_depth = params[1].safeGet<UInt64>();
    UInt64 level_flag = params[2].safeGet<UInt64>();
    std::vector<UInt64> prop_flags = transformArrayIntoVector<UInt64>(params[3].safeGet<Array>());

    if (!window || !max_session_depth || !level_flag)
        throw Exception("Aggregate function " + name + "(>0, >0, >0)(...).", ErrorCodes::BAD_ARGUMENTS);

    size_t extra_prop_size = argument_types.size() - 3;
    // check extra prop type
    for (size_t i = 3; i < argument_types.size(); ++i)
    {
        DataTypePtr extra_prop_type = argument_types[i];
        if (extra_prop_type->isNullable())
            extra_prop_type = static_cast<const DataTypeNullable *>(extra_prop_type.get())->getNestedType();

        if (!isStringOrFixedString(extra_prop_type))
            throw Exception("Extra prop type should String or Nullable(String)", ErrorCodes::BAD_ARGUMENTS);
    }
    // check extra prop size
    size_t total_size_in_flag = 0;
    for (const auto & prop_flag : prop_flags)
        total_size_in_flag += __builtin_popcount(prop_flag);

    if (total_size_in_flag > extra_prop_size)
        throw Exception("Extra prop amount is less than the number specified in prop_flag", ErrorCodes::BAD_ARGUMENTS);

    if constexpr (is_by_times)
        return std::make_shared<AggregateFunctionFunnelPathSplitByTimes<is_terminating_event, deduplicate>>(
            window, max_session_depth, level_flag, prop_flags, argument_types, params);
    else
        return std::make_shared<AggregateFunctionFunnelPathSplit<is_terminating_event, deduplicate>>(
            window, max_session_depth, level_flag, prop_flags, argument_types, params);
}

void registerAggregateFunctionFunnelPathSplit(AggregateFunctionFactory & factory)
{
    auto regist_function = [&](String && name, bool is_by_times) {
        static_for<0, 2 * 2>([&](auto ij) {
            String func_name = name;
            constexpr bool i = ij & (1 << 0);
            constexpr bool j = ij & (1 << 1);
            func_name += (i ? "R" : "");
            func_name += (j ? "D" : "");
            if (is_by_times)
                factory.registerFunction(func_name, createAggregateFunctionFunnelPathSplit<i, j, true>, AggregateFunctionFactory::CaseSensitive);
            else
                factory.registerFunction(func_name, createAggregateFunctionFunnelPathSplit<i, j, false>, AggregateFunctionFactory::CaseSensitive);
        });
    };

    regist_function("funnelPathSplit", false);
    regist_function("funnelPathSplitByTimes", true);
}

}
