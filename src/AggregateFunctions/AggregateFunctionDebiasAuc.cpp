//
// Created by zouyang.233@bytedance.com on 2021/7/149
//

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionDebiasAuc.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnArray.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

AggregateFunctionPtr
createAggregateFunctionDebiasAuc(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertTernary(name, argument_types);

    // Default parameters
    Float64 precision = 0.00001;
    Float64 min = 0.0;
    Float64 max = 1.0;

    if (parameters.size() > 0) {
        auto type = parameters[0].getType();
        if (type != Field::Types::Float64)
            throw Exception("Parameter precision for aggregate function " + name + " should be a float" , ErrorCodes::BAD_ARGUMENTS);
        precision = parameters[0].get<Float64>();
        if (precision <= 0.0)
            throw Exception("Parameter precision for aggregate function " + name + " should be larger than 0", ErrorCodes::BAD_ARGUMENTS);
    }

    if (parameters.size() > 1) {
        auto type = parameters[1].getType();
        if (type != Field::Types::Float64)
            throw Exception("Parameter min for aggregate function " + name + " should be a float", ErrorCodes::BAD_ARGUMENTS);
        min = parameters[1].get<Float64>();
    }

    if (parameters.size() > 2) {
        auto type = parameters[2].getType();
        if (type != Field::Types::Float64)
            throw Exception("Parameter max for aggregate function " + name + " should be a float", ErrorCodes::BAD_ARGUMENTS);
        max = parameters[2].get<Float64>();
        if (max <= min)
            throw Exception("Parameter max should be larger than min for aggregate function " + name + " should be a float", ErrorCodes::BAD_ARGUMENTS);
    }

    AggregateFunctionPtr res(createWithThreeNumericTypes<FunctionDebiasAuc>(
        *argument_types[0], *argument_types[1], *argument_types[2], argument_types, parameters, precision, min, max));
    if (!res)
       throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
                        + " and " + argument_types[2]->getName() + " of arguments for aggregate function " 
                        + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

} // end of anonymous namespace

void registerAggregateFunctionDebiasAuc(AggregateFunctionFactory & factory)
{
    factory.registerFunction("debiasAuc", createAggregateFunctionDebiasAuc, AggregateFunctionFactory::CaseInsensitive);
}

} // end of namespace DB
