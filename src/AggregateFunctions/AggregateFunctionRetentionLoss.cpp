#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionRetentionLoss.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{
namespace
{
    AggregateFunctionPtr
    createAggregateFunctionRetentionLoss(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * )
    {
        if (argument_types.size() != 2)
            throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * array_type = checkAndGetDataType<DataTypeArray>(argument_types[0].get());
        const auto * array_type2 = checkAndGetDataType<DataTypeArray>(argument_types[1].get());
        if (!array_type || !array_type2)
            throw Exception("arguments for function " + name + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        size_t params_size = parameters.size();

        if (params_size != 1)
            throw Exception("This instantiation of " + name + "aggregate function doesn't accept " + toString(params_size) + " parameters.", ErrorCodes::LOGICAL_ERROR);

        UInt64 window = parameters[0].safeGet<UInt64>();

        AggregateFunctionPtr res = nullptr;

        if (argument_types[0]->getName() == "Array(Array(String))")
        {
            res = std::make_shared<AggregateFunctionAttrRetentionLoss>(window, argument_types, parameters);
        }
        else
        {
            res = AggregateFunctionPtr(createWithIntegerTypeLastKnown<AggregateFunctionRetentionLoss, compress_trait<COMPRESS_BIT>>(
                *array_type->getNestedType(), window, argument_types, parameters));
        }

        if (!res)
            throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res;
    }
}

void registerAggregateFunctionRetentionLoss(AggregateFunctionFactory & factory)
{
    factory.registerFunction("retentionLoss", createAggregateFunctionRetentionLoss, AggregateFunctionFactory::CaseInsensitive);
}

}
