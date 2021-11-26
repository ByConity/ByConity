#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUserDistribution.h>
#include <AggregateFunctions/Helpers.h>

namespace DB {
namespace {

    AggregateFunctionPtr createAggregateFunctionUserDistributionHelper(const std::string &name, const DataTypes &argument_types, const Array &params)
    {
        if (params.size() != 3)
            throw Exception("Aggregate fucntion " + name + " requires (start_time, time_granularity, num_slots).", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (argument_types.size() < 2)
            throw Exception(
                "Incorrect number of arguments for aggregate function " + name + ", should be at least 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!argument_types[0]->equals(*argument_types[1]))
            throw Exception("First two columns should be the same type for aggregate function " + name,
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 start_time = params[0].safeGet<UInt64>();
        UInt64 time_granularity = params[1].safeGet<UInt64>();
        UInt64 num_slots = params[2].safeGet<UInt64>();

        if (time_granularity == 0)
            throw Exception("The Parameter 'time_granularity' should not be zero!", ErrorCodes::LOGICAL_ERROR);

        AggregateFunctionPtr res = nullptr;

        res.reset(createWithNumericType<AggregateFunctionUserDistribution>(
            *argument_types[0], start_time, time_granularity, num_slots, argument_types, params
            ));

        if (!res)
            throw Exception("Illegal type for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res;

    }

    AggregateFunctionPtr createAggregateFunctionUserDistribution(const std::string &name, const DataTypes &argument_types, const Array &params, const Settings * )
    {
        return createAggregateFunctionUserDistributionHelper(name, argument_types, params);
    }
}

void registerAggregateFunctionUserDistribution(AggregateFunctionFactory & factory)
{
    factory.registerFunction("userDistribution",
                             createAggregateFunctionUserDistribution,
                             AggregateFunctionFactory::CaseSensitive);
}
}
