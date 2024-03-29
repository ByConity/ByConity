#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct ErfcName { static constexpr auto name = "erfc"; };
using FunctionErfc = FunctionMathUnary<UnaryFunctionPlain<ErfcName, std::erfc>>;

}

REGISTER_FUNCTION(Erfc)
{
    factory.registerFunction<FunctionErfc>();
}

}
