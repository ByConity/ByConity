#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFastAuc2.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnArray.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionFastAuc2(const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings *)
{
    assertBinary(name, arguments);

    // Default parameters
    Float64 min = 0.0;
    Float64 max = 1.0;
    Float64 precision = 0.00001;

    if (!parameters.empty())
    {
        if (parameters[0].getType() != Field::Types::Float64)
            throw Exception("Parameter precision for aggregate function " + name + " should be a float" , ErrorCodes::BAD_ARGUMENTS);

        precision = parameters[0].get<Float64>();
        if (precision <= 0.0)
            throw Exception("Parameter precision for aggregate function " + name + " should be larger than 0", ErrorCodes::BAD_ARGUMENTS);
    }

    if (parameters.size() > 1)
    {
        if (parameters[1].getType() != Field::Types::Float64)
            throw Exception("Parameter min for aggregate function " + name + " should be a float", ErrorCodes::BAD_ARGUMENTS);
        min = parameters[1].get<Float64>();
    }

    if (parameters.size() > 2)
    {
        if (parameters[2].getType() != Field::Types::Float64)
            throw Exception("Parameter max for aggregate function " + name + " should be a float", ErrorCodes::BAD_ARGUMENTS);
        max = parameters[2].get<Float64>();
        if (max <= min)
            throw Exception("Parameter max should be larger than min for aggregate function " + name + " should be a float", ErrorCodes::BAD_ARGUMENTS);
    }

    AggregateFunctionPtr res(createWithTwoNumericTypes<FunctionFastAuc2>(*arguments[0], *arguments[1], arguments, parameters, precision, min, max));

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal types {} and {}  of arguments for aggregate function {}",
            arguments[0]->getName(), arguments[1]->getName(), name);
    return res;
}

}

void registerAggregateFunctionFastAuc2(AggregateFunctionFactory & factory)
{
    factory.registerFunction("fastAuc2", createAggregateFunctionFastAuc2, AggregateFunctionFactory::CaseInsensitive);
}

}
