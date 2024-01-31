#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"
#include "EscapeLike.h"

namespace DB
{
namespace
{

struct NameRLike
{
    static constexpr auto name = "rlike";
};

using RLikeImpl = MatchImpl<NameRLike, MatchTraits::Syntax::Re2, MatchTraits::Case::Sensitive, MatchTraits::Result::DontNegate>;
using FunctionRLike = FunctionsStringSearch<RLikeImpl>;

using FunctionEscapeRLike = FunctionsStringSearch<EscapeRLikeImpl>;

}

REGISTER_FUNCTION(RLike)
{
    factory.registerFunction<FunctionRLike>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionEscapeRLike>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("regexp", "rlike", FunctionFactory::CaseInsensitive);
}

}
