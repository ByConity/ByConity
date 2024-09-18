//
// Created by 袁宇豪 on 9/18/22.
//

#include "AggregateFunctionCountByGranularity.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <Common/FieldVisitors.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

    AggregateFunctionPtr createAggregateFunctionCountByGranularity
        (const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
    {
        if (argument_types.size() != 1)
            throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const IDataType & argument_type = *argument_types[0];
        WhichDataType which(argument_type);

        if
            (
                which.isNothing()
                || which.isArray()
                || which.isFunction()
                || which.isAggregateFunction()
                || which.isMap()
                || which.isBitmap64()
                || which.isSet()
                || which.isTuple()
                || which.isInterval()
                || which.isDecimal()
                || which.isInt128()
                || which.isUInt128()
            )
        {
            throw Exception("argument of " + name + " can not be "
                                                    "(Nothing,Array,Function,"
                                                    "AggregateFunction,Map,Bitmap64,"
                                                    "Set,Tuple,Interval,"
                                                    "Decimal,Int128,UInt128)", ErrorCodes::BAD_ARGUMENTS);
        }
        else if (which.isStringOrFixedString())
        {
            //auto a =AggregateFunctionCountByGranularity<String>(argument_types, params);
            return std::make_shared<AggregateFunctionCountByGranularity<String>>(argument_types, params);
        }
        else if (which.isInt8())
        {
            auto a =AggregateFunctionCountByGranularity<Int8>(argument_types, params);
            return std::make_shared<AggregateFunctionCountByGranularity<Int8>>(argument_types, params);
        }
        else if (which.isUInt8() || which.isEnum8())
        {
            return std::make_shared<AggregateFunctionCountByGranularity<UInt8>>(argument_types, params);
        }
        else if (which.isInt16())
        {
            return std::make_shared<AggregateFunctionCountByGranularity<Int16>>(argument_types, params);
        }
        else if (which.isUInt16() || which.isEnum16())
        {
            return std::make_shared<AggregateFunctionCountByGranularity<UInt16>>(argument_types, params);
        }
        else if (which.isInt32())
        {
            return std::make_shared<AggregateFunctionCountByGranularity<Int32>>(argument_types, params);
        }
        else if (which.isUInt32() || which.isDateTime())
        {
            return std::make_shared<AggregateFunctionCountByGranularity<UInt32>>(argument_types, params);
        }
        else if (which.isInt64())
        {
            return std::make_shared<AggregateFunctionCountByGranularity<Int64>>(argument_types, params);
        }
        else if (which.isUInt64())
        {
            return std::make_shared<AggregateFunctionCountByGranularity<UInt64>>(argument_types, params);
        }
        //        TODO can't support Int128 for now
        //        else if (which.isInt128())
        //        {
        //            return std::make_shared<AggregateFunctionCountByGranularity<Int128>>(argument_types, params);
        //        }
        else if (which.isUInt128())
        {
            return std::make_shared<AggregateFunctionCountByGranularity<UInt128>>(argument_types, params);
        }
        else if (which.isFloat32())
        {
            return std::make_shared<AggregateFunctionCountByGranularity<Float32>>(argument_types, params);
        }
        else if (which.isFloat64())
        {
            return std::make_shared<AggregateFunctionCountByGranularity<Float64>>(argument_types, params);
        }
        // TODO can't support Decimal for now
        //        else if (which.isDecimal32())
        //        {
        //            return std::make_shared<AggregateFunctionCountByGranularity<Decimal32>>(argument_types, params);
        //        }
        //        else if (which.isDecimal64() || which.isDateTime64())
        //        {
        //            return std::make_shared<AggregateFunctionCountByGranularity<Decimal64>>(argument_types, params);
        //        }
        //        else if (which.isDecimal128())
        //        {
        //            return std::make_shared<AggregateFunctionCountByGranularity<Decimal128>>(argument_types, params);
        //        }
        else
        {
            return std::make_shared<AggregateFunctionCountByGranularity<String>>(argument_types, params);
        }

        __builtin_unreachable();
    }
}

void registerAggregateFunctionCountByGranularity(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };
    factory.registerFunction("countByGranularity", {createAggregateFunctionCountByGranularity, properties}, AggregateFunctionFactory::CaseInsensitive);
}

}
