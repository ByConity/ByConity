#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionPathCount.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

AggregateFunctionPtr createAggregateFunctionPathCount(const String & name, const DataTypes & argument_types, const Array & params, const Settings * )
{
    if (params.size() != 2)
        throw Exception("Aggregate function " + name + " requires 2 parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    //TODO check arguments

    UInt64 max_node_size = params[0].safeGet<UInt64>();
    UInt64 max_step_size = params[1].safeGet<UInt64>();

    if (!max_node_size || max_step_size < 2)
        throw Exception("Aggregate function " + name + "(>0, >1)(...)", ErrorCodes::BAD_ARGUMENTS);

    if (argument_types.size() == 1 && argument_types[0]->getName() == "Array(Array(Tuple(UInt16, String)))")
        return std::make_shared<AggregateFunctionPathCount<UInt16, true>>(max_node_size, max_step_size, argument_types, params);

    if (argument_types.size() == 3)
        return std::make_shared<AggregateFunctionPathCount<UInt16, false>>(max_node_size, max_step_size, argument_types, params);

    throw Exception("Aggregate function " + name + "(...)([[(UInt16, String)]]) or " + name + "(...)([(UInt16, String)], UInt64, UInt64).", ErrorCodes::BAD_ARGUMENTS);
}

void registerAggregateFunctionPathCount(AggregateFunctionFactory & factory)
{
    factory.registerFunction("pathCount", createAggregateFunctionPathCount, AggregateFunctionFactory::CaseSensitive);
}

}
