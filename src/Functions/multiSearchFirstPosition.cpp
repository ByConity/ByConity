#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstPositionImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstPosition
{
    static constexpr auto name = "multiSearchFirstPosition";
};

using FunctionMultiSearchFirstPosition
    = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<PositionCaseSensitiveASCII>, NameMultiSearchFirstPosition>;

}

REGISTER_FUNCTION(MultiSearchFirstPosition)
{
    factory.registerFunction<FunctionMultiSearchFirstPosition>();
}

}
