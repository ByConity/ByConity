#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFastPrevAuc2.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnArray.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionFastPrevAuc2(const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings *)
{
    assertBinary(name, arguments);

    // Default parameters
    Float64 min = 0.0;
    Float64 max = 1.0;
    Float64 precision = 0.00001;
    // 1: serialize into binary
    // 0: serialize into String
    UInt8 use_string_state = 0;

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

    if (parameters.size() > 3)
        use_string_state = parameters[3].get<UInt8>();

    AggregateFunctionPtr res(createWithTwoNumericTypes<FunctionFastPrevAuc2>(*arguments[0], *arguments[1], arguments, parameters, precision, min, max, use_string_state));

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal types {} and {}  of arguments for aggregate function {}",
            arguments[0]->getName(), arguments[1]->getName(), name);
    return res;
}

}

void registerAggregateFunctionFastPrevAuc2(AggregateFunctionFactory & factory)
{
    factory.registerFunction("fastPrevAuc2", createAggregateFunctionFastPrevAuc2, AggregateFunctionFactory::CaseInsensitive);
}

}
