#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct ErfName { static constexpr auto name = "erf"; };
using FunctionErf = FunctionMathUnary<UnaryFunctionPlain<ErfName, std::erf>>;

}

REGISTER_FUNCTION(Erf)
{
    factory.registerFunction<FunctionErf>();
}

}
