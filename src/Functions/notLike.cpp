#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"
#include "EscapeLike.h"

namespace DB
{
namespace
{

struct NameNotLike
{
    static constexpr auto name = "notLike";
};

using NotLikeImpl = MatchImpl<NameNotLike, MatchTraits::Syntax::Like, MatchTraits::Case::Sensitive, MatchTraits::Result::Negate>;
using FunctionNotLike = FunctionsStringSearch<NotLikeImpl>;
using FunctionEscapeNotLike = FunctionsStringSearch<EscapeNotLike>;

}

REGISTER_FUNCTION(NotLike)
{
    factory.registerFunction<FunctionNotLike>();
    factory.registerFunction<FunctionEscapeNotLike>();
}

}
