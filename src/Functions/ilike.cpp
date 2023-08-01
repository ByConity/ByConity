#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"
#include "EscapeMatchImpl.h"

namespace DB
{
namespace
{

struct NameILike
{
    static constexpr auto name = "ilike";
};

struct NameEscapeILike
{
    static constexpr auto name = "escapeILike";
};

using ILikeImpl = MatchImpl<true, false, /*case-insensitive*/true>;
using FunctionILike = FunctionsStringSearch<ILikeImpl, NameILike>;

using EscapeILikeImpl = EscapeMatchImpl<true, false, /*case-insensitive*/true>;
using FunctionEscapeILike = FunctionsStringSearch<EscapeILikeImpl, NameEscapeILike>;

}

void registerFunctionILike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionILike>();
    factory.registerFunction<FunctionEscapeILike>();
}

}
