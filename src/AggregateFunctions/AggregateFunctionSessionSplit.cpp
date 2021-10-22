#include <AggregateFunctions/AggregateFunctionSessionSplit.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeNullable.h>
namespace DB
{

namespace
{

void checkArgumentTypes(const String & name, const DataTypes & argument_types)
{
    static constexpr size_t max_session_argument_size = 20;
    size_t argument_size = argument_types.size();

    if(argument_size > max_session_argument_size)
        throw Exception("Aggregate function " + name + "has to many parameter, max is 20.", ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

    String types = "(UInt64, String, UInt64, Nullable(Int64/UInt64), Nullable(Int64/UInt64)" ;
    for (size_t i = 0; i < argument_size - 5; ++i)
        types += ", Nullable(String)";
    types += ")";

    if (!typeid_cast<const DataTypeUInt64 *>(argument_types[0].get()))
        throw Exception("Aggregate function " + name + types + " type not matched in line 0!", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (!typeid_cast<const DataTypeString *>(argument_types[1].get()))
        throw Exception("Aggregate function " + name + types + " type not matched in line 1!", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (!typeid_cast<const DataTypeUInt64 *>(argument_types[2].get()))
        throw Exception("Aggregate function " + name + types + " type not matched in line 2!", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    auto type_bit64 = [](const DataTypePtr & type) -> bool
    {
        return typeid_cast<const DataTypeUInt64 *>(type.get()) || typeid_cast<const DataTypeInt64 *>(type.get());
    };

    for (size_t i = 3; i < argument_size; ++i)
    {
        const DataTypeNullable * nullable = typeid_cast<const DataTypeNullable *>(argument_types[i].get());
        if (!nullable ||
            (i >= 5 && !typeid_cast<const DataTypeString *>(nullable->getNestedType().get())) ||
            (i < 5 && !type_bit64(nullable->getNestedType())))
        {
            throw Exception("Aggregate function " + name + types + " type not matched in line " + std::to_string(i) + "!", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }
}

AggregateFunctionPtr createAggregateFunctionSessionSplit(const String & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (params.size() != 4)
        throw Exception("Aggregate function " + name + " requires 4 parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() < 7)
        throw Exception("Aggregate function " + name + " requires not less than 7 arguments, but parsed " + toString(argument_types.size()), ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    checkArgumentTypes(name, argument_types);

    UInt64 watch_start = params[0].safeGet<UInt64>();
    UInt64 window_size = params[1].safeGet<UInt64>();
    UInt64 base_time = params[2].safeGet<UInt64>();
    UInt8 type = static_cast<UInt8 >(params[3].safeGet<UInt64>());

    if (type >= 2)
        throw Exception("Aggregate function " + name + " fourth parameter must be 0 or 1", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (!window_size)
        throw Exception("Aggregate function " + name + " second parameter should not 0. It should be day(86400), week(604800) or month.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<AggregateFunctionSessionSplit>(watch_start, window_size, base_time, type, argument_types, params);
}

AggregateFunctionPtr createAggregateFunctionSessionSplitR2(const String & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (params.size() != 4)
        throw Exception("Aggregate function " + name + " requires 4 parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() != 7)
        throw Exception("Aggregate function " + name + " requires " + std::to_string(7) + " arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    checkArgumentTypes(name, argument_types);

    UInt64 watch_start = params[0].safeGet<UInt64>();
    UInt64 window_size = params[1].safeGet<UInt64>();
    UInt64 base_time = params[2].safeGet<UInt64>();
    UInt8 type = static_cast<UInt8 >(params[3].safeGet<UInt64>());

    if (type > 2)
        throw Exception("Aggregate function " + name + " fourth parameter must be less than 3", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    if (!window_size)
        throw Exception("Aggregate function " + name + " second parameter should be day(86400), week(604800) or month.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<AggregateFunctionSessionSplitR2>(watch_start, window_size, base_time, type, argument_types, params);
}

AggregateFunctionPtr createAggregateFunctionPageTime(const String & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (params.size() != 3 && params.size() != 4)
        throw Exception("Aggregate function " + name + " requires 3 or 4 parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() != 7)
        throw Exception("Aggregate function " + name + " requires " + std::to_string(7) + " arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    checkArgumentTypes(name, argument_types);

    UInt64 watch_start = params[0].safeGet<UInt64>();
    UInt64 window_size = params[1].safeGet<UInt64>();
    UInt64 base_time = params[2].safeGet<UInt64>();
    String refer_url = "all";
    if (params.size() == 4)
        refer_url = params[3].safeGet<String>();

    if (!window_size)
        throw Exception("Aggregate function " + name + " second parameter should be day(86400), week(604800) or month.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<AggregateFunctionPageTime>(watch_start, window_size, base_time, refer_url, argument_types, params);
}

AggregateFunctionPtr createAggregateFunctionPageTime2(const String & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (params.size() != 3)
        throw Exception("Aggregate function " + name + " requires 3 parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() < 7)
        throw Exception("Aggregate function " + name + " requires not less than 7 arguments, but parsed " + toString(argument_types.size()), ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    checkArgumentTypes(name, argument_types);

    UInt64 watch_start = params[0].safeGet<UInt64>();
    UInt64 window_size = params[1].safeGet<UInt64>();
    UInt64 base_time = params[2].safeGet<UInt64>();

    if (!window_size)
        throw Exception("Aggregate function " + name + " second parameter should be day(86400), week(604800) or month.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<AggregateFunctionPageTime2>(watch_start, window_size, base_time, argument_types, params);
}

AggregateFunctionPtr createAggregateFunctionSumMetric(const String & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception("Aggregate function " + name + " requires 1 argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    // check input type
    if (!typeid_cast<const DataTypeTuple *>(argument_types[0].get()))
        throw Exception("Aggregate function " + name + " Tuple type not matched!", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<AggregateFunctionSumMetric>(argument_types, params);
}

}

void registerAggregateFunctionSessionSplit(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sessionSplit", createAggregateFunctionSessionSplit, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("sessionSplitR2", createAggregateFunctionSessionSplitR2, AggregateFunctionFactory::CaseInsensitive);

    factory.registerFunction("sumMetric", createAggregateFunctionSumMetric, AggregateFunctionFactory::CaseInsensitive);

    factory.registerFunction("pageTime", createAggregateFunctionPageTime, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("pageTime2", createAggregateFunctionPageTime2, AggregateFunctionFactory::CaseInsensitive);
}

}

