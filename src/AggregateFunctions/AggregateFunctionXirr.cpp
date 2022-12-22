#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionXirr.h>


namespace DB
{

namespace
{
    AggregateFunctionPtr createAggregateFunctionXirr(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * )
    {
        AggregateFunctionPtr res;

        if (argument_types.size() < 2)
            throw Exception("Aggregate function " + name + " requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        std::optional<Float64> guess = std::nullopt;
        if (!parameters.empty())
        {
            const auto & param = parameters[0];
            if (param.getType() == Field::Types::Float64)
                guess = param.get<Float64>();
            else if (isInt64OrUInt64FieldType(param.getType()))
                guess = param.get<Int64>();
        }

        DataTypePtr data_type_0 = argument_types[0], data_type_1 = argument_types[1];
        if (isNumber(data_type_0) && isNumber(data_type_1))
            return AggregateFunctionPtr(createWithTwoNumericTypes<AggregateFunctionXirr>(*data_type_0, *data_type_1, argument_types, parameters, guess));

        throw Exception("Aggregate function " + name + " requires two numeric arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

}

void registerAggregateFunctionXirr(AggregateFunctionFactory & factory)
{
    factory.registerFunction("xirr", createAggregateFunctionXirr, AggregateFunctionFactory::CaseInsensitive);
}

}
