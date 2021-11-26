#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUserDistributionMonthly.h>
#include <AggregateFunctions/Helpers.h>

#include <DataTypes/DataTypeString.h>

namespace DB
{
namespace
{

    AggregateFunctionPtr createAggregateFunctionUserDistributionMonthlyHelper(const std::string &name, const DataTypes &argument_types, const Array &params)
    {
        if (params.size() != 3)
            throw Exception("Aggregate fucntion " + name + " requires (time_zone, start_month, num_slots).",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (argument_types.size() < 2)
            throw Exception(
                "Incorrect number of arguments for aggregate function " + name + ", should be at least 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!argument_types[0]->equals(*argument_types[1]))
            throw Exception("First two columns should be the same type for aggregate function " + name,
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto & time_zone = params[0].safeGet<std::string>();
        UInt64 start_time = params[1].safeGet<UInt64>();
        UInt64 num_slots = params[2].safeGet<UInt64>();

        AggregateFunctionPtr res = nullptr;

        res.reset(createWithNumericType<AggregateFunctionUserDistributionMonthly>(
            *argument_types[0], time_zone, start_time, num_slots, argument_types, params
            ));

        if (!res)
            throw Exception("Illegal type for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res;

    }

    AggregateFunctionPtr createAggregateFunctionUserDistributionMonthly(const std::string &name, const DataTypes &argument_types, const Array &params, const Settings * )
    {
        return createAggregateFunctionUserDistributionMonthlyHelper(name, argument_types, params);
    }
}

void registerAggregateFunctionUserDistributionMonthly(AggregateFunctionFactory & factory)
{
    factory.registerFunction("userDistributionMonthly",
                             createAggregateFunctionUserDistributionMonthly,
                             AggregateFunctionFactory::CaseSensitive);
}
}
