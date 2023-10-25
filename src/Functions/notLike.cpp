#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"
#include "EscapeMatchImpl.h"

namespace DB
{
namespace
{

struct NameNotLike
{
    static constexpr auto name = "notLike";
};

struct NameEscapeNotLike
{
    static constexpr auto name = "escapeNotLike";
};

using FunctionNotLike = FunctionsStringSearch<MatchImpl<true, true>, NameNotLike>;
using FunctionEscapeNotLike = FunctionsStringSearch<EscapeMatchImpl<true, true>, NameEscapeNotLike>;

}

REGISTER_FUNCTION(NotLike)
{
    factory.registerFunction<FunctionNotLike>();
    factory.registerFunction<FunctionEscapeNotLike>();
}

}
