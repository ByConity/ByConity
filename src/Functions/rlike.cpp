#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"
#include "EscapeMatchImpl.h"

namespace DB
{
namespace
{

struct NameRLike
{
    static constexpr auto name = "rlike";
};

struct NameEscapeRLike
{
    static constexpr auto name = "escapeRLike";
};

using RLikeImpl = MatchImpl</*SQL LIKE */ false, /*revert*/false>;
using FunctionRLike = FunctionsStringSearch<RLikeImpl, NameRLike>;

using EscapeRLikeImpl = EscapeMatchImpl</*SQL LIKE */ false, /*revert*/false>;
using FunctionEscapeRLike = FunctionsStringSearch<EscapeRLikeImpl, NameEscapeRLike>;

}

REGISTER_FUNCTION(RLike)
{
    factory.registerFunction<FunctionRLike>();
    factory.registerFunction<FunctionEscapeRLike>();
    factory.registerAlias("regexp", "rlike", FunctionFactory::CaseInsensitive);
}

}
