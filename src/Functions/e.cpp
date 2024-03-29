#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathConstFloat64.h>

namespace DB
{
namespace
{

struct EImpl
{
    static constexpr auto name = "e";
    static constexpr double value = 2.7182818284590452353602874713526624977572470;
};

using FunctionE = FunctionMathConstFloat64<EImpl>;

}

REGISTER_FUNCTION(E)
{
    factory.registerFunction<FunctionE>();
}

}
