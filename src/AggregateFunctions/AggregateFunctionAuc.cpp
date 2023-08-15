#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAuc.h>
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
createAggregateFunctionAuc(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    // Default parameters
    bool is_regression = false;
    Int64 num_reg_sample = 1000000;

    if (parameters.size() > 0) {
        is_regression = parameters[0].safeGet<UInt64>() != 0;
    }

    if (parameters.size() > 1) {
        num_reg_sample = parameters[1].safeGet<UInt64>();
    }

    AggregateFunctionPtr res(createWithTwoNumericTypes<FunctionAuc>(*argument_types[0], *argument_types[1], argument_types, is_regression, num_reg_sample));
    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
                            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

} // end of anonymous namespace

void registerAggregateFunctionAuc(AggregateFunctionFactory & factory)
{
    factory.registerFunction("auc", createAggregateFunctionAuc, AggregateFunctionFactory::CaseInsensitive);
}

} // end of namespace DB
