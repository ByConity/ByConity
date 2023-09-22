#include <AggregateFunctions/AggregateFunctionNdcg.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
    namespace
    {
        AggregateFunctionPtr
        createAggregateFunctionNdcg(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            assertQuaternary(name, argument_types);

            AggregateFunctionPtr res(createWithFourNumericTypes<AggregateFunctionNdcg>(*argument_types[0], *argument_types[1], *argument_types[2], *argument_types[3], argument_types));
            if (!res)
                throw Exception(
                        "Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName() +
                        " and " + argument_types[2]->getName() + " and " + argument_types[3]->getName()
                        + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            return res;
        }
    }

    void registerAggregateFunctionNdcg(AggregateFunctionFactory & factory)
    {
        factory.registerFunction("ndcg", createAggregateFunctionNdcg, AggregateFunctionFactory::CaseInsensitive);
    }
}
