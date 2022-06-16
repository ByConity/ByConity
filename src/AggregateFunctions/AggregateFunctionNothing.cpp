#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

namespace
{
    AggregateFunctionPtr createAggregateFunctionNothing(
        const std::string & /*name*/, const DataTypes & argument_types, const Array & parameters, const Settings * /*settings*/)
    {
        return std::make_shared<AggregateFunctionNothing>(argument_types, parameters);
    }
}

void registerAggregateFunctionNothing(AggregateFunctionFactory & factory)
{
    factory.registerFunction("nothing", createAggregateFunctionNothing);
}

}
