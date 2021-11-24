#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFunnelRep.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

    AggregateFunctionPtr createAggregateFunctionFunnelRep(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        if (argument_types.size() != 1)
            throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * array_type = checkAndGetDataType<DataTypeArray>(argument_types[0].get());
        if (!array_type)
            throw Exception("First argument for function " + name + " must be an array.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (parameters.size() != 2)
            throw Exception("Aggregate funnction " + name + " requires (number_check, number_event).", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 watch_numbers = parameters[0].safeGet<UInt64>();
        UInt64 event_numbers = parameters[1].safeGet<UInt64>();

        AggregateFunctionPtr res(createWithIntegerType<AggregateFunctionFunnelRep>(*array_type->getNestedType(),
                                                                                   watch_numbers, event_numbers,
                                                                                   argument_types, parameters));
        if (!res)
            throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return res;
    }
}

void registerAggregateFunctionFunnelRep(AggregateFunctionFactory & factory)
{
    factory.registerFunction("funnelRep", createAggregateFunctionFunnelRep, AggregateFunctionFactory::CaseInsensitive);
}

}
