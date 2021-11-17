#include <AggregateFunctions/AggregateFunctionGenArrayMonth.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionGenArrayMonth(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (parameters.size() != 2)
        throw Exception("Aggregate function " + name + " require 2 parameters.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    if (argument_types.size() != 1 && argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    if (!isNumber(*argument_types[0]))
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res = nullptr;
    UInt64 num_steps = parameters[0].safeGet<UInt64>();
    String date_start = parameters[1].safeGet<String>();

    if (argument_types.size() == 2)
        res = AggregateFunctionPtr(createWithTwoTypes<AggregateFunctionAttrGenArrayMonth>(*argument_types[0], *argument_types[1], num_steps, date_start, argument_types, parameters));
    else
        res = AggregateFunctionPtr(createWithIntegerType<AggregateFunctionGenArrayMonth>(*argument_types[0], num_steps, date_start, argument_types, parameters));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

void registerAggregateFunctionGenArrayMonth(AggregateFunctionFactory & factory)
{
    factory.registerFunction("genArrayMonth", createAggregateFunctionGenArrayMonth, AggregateFunctionFactory::CaseInsensitive);
}

}

