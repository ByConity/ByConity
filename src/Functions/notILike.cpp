#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"
#include "EscapeMatchImpl.h"

namespace DB
{
namespace
{

struct NameNotILike
{
    static constexpr auto name = "notILike";
};

struct NameEscapeNotILike
{
    static constexpr auto name = "escapeNotILike";
};

using NotILikeImpl = MatchImpl<true, true, /*case-insensitive*/true>;
using FunctionNotILike = FunctionsStringSearch<NotILikeImpl, NameNotILike>;

using EscapeNotILikeImpl = EscapeMatchImpl<true, true, /*case-insensitive*/true>;
using FunctionEscapeNotILike = FunctionsStringSearch<EscapeNotILikeImpl, NameEscapeNotILike>;

}

REGISTER_FUNCTION(NotILike)
{
    factory.registerFunction<FunctionNotILike>();
    factory.registerFunction<FunctionEscapeNotILike>();
}
}
