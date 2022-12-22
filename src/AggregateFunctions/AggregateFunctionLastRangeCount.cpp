#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionLastRangeCount.h>
#include <AggregateFunctions/Helpers.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
namespace
{
    AggregateFunctionPtr createAggregateFunctionLastRangeCountHelper(const std::string &name, const DataTypes &argument_types, const Array &params)
    {
        if (params.size() != 3)
            throw Exception("Aggregate fucntion " + name + " requires (duration, start_index, num_slots).",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (argument_types.size() != 1)
            throw Exception(
                "Incorrect number of arguments for aggregate function " + name + ", should be 1",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!typeid_cast<const DataTypeArray *>(argument_types[0].get()))
            throw Exception("Aggregate function " + name +" Array type not matched!",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        UInt64 duration = params[0].safeGet<UInt64>();
        UInt64 start_index = params[1].safeGet<UInt64>();
        UInt64 num_slots = params[2].safeGet<UInt64>();

        const auto * array_type = checkAndGetDataType<DataTypeArray>(argument_types[0].get());
        if (!array_type)
            throw Exception("First argument for function " + name + " must be an array.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        AggregateFunctionPtr res = nullptr;

        res.reset(createWithIntegerType<AggregateFunctionLastRangeCount>(
            *array_type->getNestedType(), duration, start_index, num_slots, argument_types, params
            ));

        return res;
    }

    AggregateFunctionPtr createAggregateFunctionLastRangeCount(const std::string &name, const DataTypes &argument_types, const Array &params, const Settings *)
    {
        return createAggregateFunctionLastRangeCountHelper(name, argument_types, params);
    }
}

void registerAggregateFunctionLastRangeCount(AggregateFunctionFactory & factory)
{
    factory.registerFunction("lastRangeCount",
                             createAggregateFunctionLastRangeCount,
                             AggregateFunctionFactory::CaseSensitive);
}
}
