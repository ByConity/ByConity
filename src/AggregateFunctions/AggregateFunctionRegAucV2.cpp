//
// Created by zouyang.233@bytedance.com on 2022/11/21.
//

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionRegAucV2.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

AggregateFunctionPtr
createAggregateFunctionRegAucV2(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    // Default parameters
    Int64 num_reg_sample = 1000000;
    Int64 flag = 1;

    if (parameters.size() > 0) {
        num_reg_sample = parameters[0].safeGet<UInt64>();
    }

    if (parameters.size() > 1) {
        flag = parameters[1].safeGet<UInt64>();
    }

    AggregateFunctionPtr res(createWithTwoNumericTypes<FunctionRegAucV2>(*argument_types[0], *argument_types[1], argument_types, num_reg_sample, flag));
    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
                            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

} // end of anonymous namespace

void registerAggregateFunctionRegAucV2(AggregateFunctionFactory & factory)
{
    factory.registerFunction("regression_auc2", createAggregateFunctionRegAucV2, AggregateFunctionFactory::CaseInsensitive);
}

} // end of namespace DB
