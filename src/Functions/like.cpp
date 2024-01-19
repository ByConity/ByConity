#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"
#include "EscapeLike.h"

namespace DB
{
namespace
{

struct NameLike
{
    static constexpr auto name = "like";
};

using LikeImpl = MatchImpl<NameLike, MatchTraits::Syntax::Like, MatchTraits::Case::Sensitive, MatchTraits::Result::DontNegate>;
using FunctionLike = FunctionsStringSearch<LikeImpl>;

using FunctionEscapeLike = FunctionsStringSearch<EscapeLikeImpl>;

}

REGISTER_FUNCTION(Like)
{
    factory.registerFunction<FunctionLike>();
    factory.registerFunction<FunctionEscapeLike>();
}

}
