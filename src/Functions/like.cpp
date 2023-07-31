#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"
#include "EscapeMatchImpl.h"

namespace DB
{
namespace
{

struct NameLike
{
    static constexpr auto name = "like";
};

struct NameEscapeLike
{
    static constexpr auto name = "escapeLike";
};

using LikeImpl = MatchImpl</*SQL LIKE */ true, /*revert*/false>;
using FunctionLike = FunctionsStringSearch<LikeImpl, NameLike>;

using EscapeLikeImpl = EscapeMatchImpl</*SQL LIKE */ true, /*revert*/false>;
using FunctionEscapeLike = FunctionsStringSearch<EscapeLikeImpl, NameEscapeLike>;

}

void registerFunctionLike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLike>();
    factory.registerFunction<FunctionEscapeLike>();
}

}
