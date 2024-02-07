#include <memory>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAvg.h>
#include <AggregateFunctions/IAggregateFunctionMySql.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "DataTypes/DataTypesNumber.h"

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
bool allowType(const DataTypePtr& type) noexcept
{
    const WhichDataType t(type);
    return t.isInt() || t.isUInt() || t.isFloat() || t.isDecimal();
}

AggregateFunctionPtr createAggregateFunctionAvg(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr& data_type = argument_types[0];

    if (!allowType(data_type) && settings && !settings->enable_implicit_arg_type_convert)
        throw Exception("Illegal type " + data_type->getName() + " of argument for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res;

    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFunctionAvg>(
            *data_type, argument_types, getDecimalScale(*data_type)));
    else
        res.reset(createWithNumericType<AggregateFunctionAvg>(*data_type, argument_types));

    if (!res && (!settings || settings->enable_implicit_arg_type_convert))
    {
        std::unique_ptr<IAggregateFunction> func(createWithNumericType<AggregateFunctionAvg>(DataTypeFloat64(), argument_types));
        res.reset(new IAggregateFunctionMySql(std::move(func)));
    }

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}
}

void registerAggregateFunctionAvg(AggregateFunctionFactory & factory)
{
    factory.registerFunction("avg", createAggregateFunctionAvg, AggregateFunctionFactory::CaseInsensitive);
}
}
