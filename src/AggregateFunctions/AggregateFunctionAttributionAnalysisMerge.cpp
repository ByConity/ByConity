#include "AggregateFunctionAttributionAnalysisMerge.h"
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{
namespace
{
    AggregateFunctionPtr createAggregateFunctionAttributionAnalysisMerge(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
    {
        if (argument_types.empty())
            throw Exception("Aggregate function " + name + " need arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (params.size() > 2)
            throw Exception("Aggregate function " + name + " need no more than two params", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 N = 0;
        if (!params.empty())
            N = params[0].safeGet<UInt64>();

        bool need_others = false;
        if (params.size() == 2)
            need_others = params[1].safeGet<UInt64>() > 0;

        const DataTypePtr & argument_type = argument_types[0];
        WhichDataType which(argument_type);
        if (which.idx == TypeIndex::Tuple)
            return std::make_shared<AggregateFunctionAttributionAnalysisTupleMerge>(N, need_others, argument_types, params);

        throw Exception("Aggregate function " + name + " need tuple arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
}

void registerAggregateFunctionAttributionAnalysisMerge(AggregateFunctionFactory & factory)
{
    factory.registerFunction("attributionAnalysisMerge", createAggregateFunctionAttributionAnalysisMerge,
                             AggregateFunctionFactory::CaseInsensitive);
}
}
