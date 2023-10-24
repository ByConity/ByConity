#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NamePositionCaseInsensitive
{
    static constexpr auto name = "positionCaseInsensitive";
};

using FunctionPositionCaseInsensitive = FunctionsStringSearch<PositionImpl<PositionCaseInsensitiveASCII>, NamePositionCaseInsensitive>;

}

REGISTER_FUNCTION(PositionCaseInsensitive)
{
    factory.registerFunction<FunctionPositionCaseInsensitive>();
}
}
