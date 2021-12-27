#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionBitmapMaxLevel.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{
namespace
{
AggregateFunctionPtr createAggregateFunctionBitMapMaxLevel(const std::string & name, const DataTypes & argument_types, const Array & parameters,const Settings *)
{
    if (parameters.size() > 2)
        throw Exception("AggregateFunction " + name + " needs 0 or 1 parameters", ErrorCodes::NOT_IMPLEMENTED);
    
    UInt64 return_tpye{0}; /// 0: only summary; 1: only detail; 2: summay + detail
    if (parameters.size() == 1)
        return_tpye = safeGet<UInt64>(parameters[0]);

    if (argument_types.size() != 2 )
        throw Exception("AggregateFunction " + name + " need two arguments", ErrorCodes::NOT_IMPLEMENTED);

    DataTypePtr data_type = argument_types[0];
    if (!WhichDataType(data_type).isInt())
        throw Exception("AggregateFunction " + name + " need signed numeric type for its first argument", ErrorCodes::NOT_IMPLEMENTED);

    if (!isBitmap64(argument_types[1]))
        throw Exception("AggregateFunction " + name + " need BitMap type for its second argument", ErrorCodes::NOT_IMPLEMENTED);

    return std::make_shared<AggregateFunctionBitMapMaxLevel>(argument_types, return_tpye);
}

}

void registerAggregateFunctionsBitmapMaxLevel(AggregateFunctionFactory & factory)
{
    factory.registerFunction("BitMapMaxLevel", createAggregateFunctionBitMapMaxLevel, AggregateFunctionFactory::CaseInsensitive);
}

}
