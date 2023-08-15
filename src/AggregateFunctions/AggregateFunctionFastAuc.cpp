#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFastAuc.h>
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
createAggregateFunctionFastAuc(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    // Default parameters
    bool is_regression = false;

    if (parameters.size() > 0) {
        is_regression = parameters[0].safeGet<UInt64>() != 0;
    }

    AggregateFunctionPtr res(createWithTwoNumericTypes<FunctionFastAuc>(*argument_types[0], *argument_types[1], argument_types, is_regression));
    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
                        + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

} // end of anonymous namespace

void registerAggregateFunctionFastAuc(AggregateFunctionFactory & factory)
{
    factory.registerFunction("fastAuc", createAggregateFunctionFastAuc, AggregateFunctionFactory::CaseInsensitive);
}

} // end of namespace DB
