#include <AggregateFunctions/AggregateFunctionSlideMatchCount.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{
namespace
{
    AggregateFunctionPtr createAggregateFunctionSlideMatchCount(const std::string &name, const DataTypes &argument_types, const Array &params, const Settings * )
    {
        if (params.size() != 3)
            throw Exception("Aggregate fucntion " + name + " requires (start_index, num_slots, include).",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (argument_types.size() != 2)
            throw Exception(
                "Incorrect number of arguments for aggregate function " + name + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!typeid_cast<const DataTypeArray *>(argument_types[0].get()) ||
            !typeid_cast<const DataTypeArray *>(argument_types[1].get()))
            throw Exception("Aggregate function " + name + " Array type not matched!",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        UInt64 start_index = params[0].safeGet<UInt64>();
        UInt64 num_slots = params[1].safeGet<UInt64>();
        UInt64 include = params[2].safeGet<UInt64>();

        const auto *array_type = checkAndGetDataType<DataTypeArray>(argument_types[0].get());
        if (!array_type)
            throw Exception("First argument for function " + name + " must be an array.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        AggregateFunctionPtr res = nullptr;

        res.reset(createWithIntegerType<AggregateFunctionSlideMatchCount>(
            *array_type->getNestedType(), start_index, num_slots, include, argument_types, params
            ));

        return res;
    }
}

void registerAggregateFunctionSlideMatchCount(AggregateFunctionFactory &factory)
{
    factory.registerFunction("slideMatchCount",
                             createAggregateFunctionSlideMatchCount,
                             AggregateFunctionFactory::CaseSensitive);
}
}

