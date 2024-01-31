#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"
#include "EscapeLike.h"

namespace DB
{
namespace
{

struct NameILike
{
    static constexpr auto name = "ilike";
};

using ILikeImpl = MatchImpl<NameILike, MatchTraits::Syntax::Like, MatchTraits::Case::Insensitive, MatchTraits::Result::DontNegate>;
using FunctionILike = FunctionsStringSearch<ILikeImpl>;

using FunctionEscapeILike = FunctionsStringSearch<EscapeILikeImpl>;

}

REGISTER_FUNCTION(ILike)
{
    factory.registerFunction<FunctionILike>();
    factory.registerFunction<FunctionEscapeILike>();
}

}
