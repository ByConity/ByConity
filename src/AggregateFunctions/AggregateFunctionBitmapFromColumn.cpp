#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionBitmapFromColumn.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{
namespace
{
AggregateFunctionPtr createAggregateFunctionBitMapFromColumn(const std::string & name, const DataTypes & argument_types, const Array & /*parameters*/, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception("AggregateFunction " + name + " need only one arguments", ErrorCodes::NOT_IMPLEMENTED);

    DataTypePtr data_type = argument_types[0];
    if (!WhichDataType(data_type).isNativeUInt() && !WhichDataType(data_type).isNativeInt())
        throw Exception("AggregateFunction " + name + " need UInt[8-64]/Int[8-64] type for its argument", ErrorCodes::NOT_IMPLEMENTED);

    return std::make_shared<AggregateFunctionBitMapFromColumn>(argument_types);
}
}

void registerAggregateFunctionsBitmapFromColumn(AggregateFunctionFactory & factory)
{
    factory.registerFunction("BitMapFromColumn", createAggregateFunctionBitMapFromColumn, AggregateFunctionFactory::CaseInsensitive);
}
}
