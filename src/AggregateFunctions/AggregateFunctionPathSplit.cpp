#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionPathSplit.h>

#include <common/constexpr_helpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

template <bool is_terminating_event = false, bool deduplicate = false>
AggregateFunctionPtr createAggregateFunctionPathSplit(const String & name, const DataTypes & argument_types, const Array & params, const Settings * )
{
    if (params.size() != 2)
        throw Exception("Aggregate function " + name + " requires 2 parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() != 3)
    {
        throw Exception(
            "Aggregate function " + name + " requires  3 arguments, but parsed " + std::to_string(argument_types.size()),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    UInt64 max_seesion_size = params[0].safeGet<UInt64>();
    UInt64 max_session_depth = params[1].safeGet<UInt64>();

    if (!max_seesion_size || !max_session_depth)
        throw Exception("Aggregate function " + name + "(>0, >0)(...).", ErrorCodes::BAD_ARGUMENTS);

    return std::make_shared<AggregateFunctionPathSplit<UInt16, is_terminating_event>>(
        max_seesion_size, max_session_depth, argument_types, params);
}

void registerAggregateFunctionPathSplit(AggregateFunctionFactory & factory)
{
    // factory.registerFunction("pathSplit", createAggregateFunctionPathSplit<false>, AggregateFunctionFactory::CaseSensitive);
    // factory.registerFunction("pathSplitR", createAggregateFunctionPathSplit<true>, AggregateFunctionFactory::CaseSensitive);

    static_for<0, 2 * 2>([&](auto ij) {
        String name = "pathSplit";
        constexpr bool i = ij & (1 << 0);
        constexpr bool j = ij & (1 << 1);
        name += (i ? "R" : "");
        name += (j ? "D" : "");
        factory.registerFunction(name, createAggregateFunctionPathSplit<i, j>, AggregateFunctionFactory::CaseSensitive);
    });
}

}
