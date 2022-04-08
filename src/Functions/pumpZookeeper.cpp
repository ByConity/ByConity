#include <Functions/FunctionFactory.h>
#include <Functions/pumpZookeeper.h>

namespace DB
{
struct NamePumpZookeeper
{
    static constexpr auto name = "pumpZookeeper";
};

using FunctionPumpZookeeper = FunctionGetHosts<NamePumpZookeeper>;

void registerFunctionPumpZookeeper(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPumpZookeeper>(FunctionFactory::CaseInsensitive);
}

}

