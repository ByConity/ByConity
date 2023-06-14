#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"

namespace DB
{
namespace
{

struct NameRLike
{
    static constexpr auto name = "rlike";
};

using RLikeImpl = MatchImpl</*SQL LIKE */ false, /*revert*/false>;
using FunctionRLike = FunctionsStringSearch<RLikeImpl, NameRLike>;

}

void registerFunctionRLike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRLike>();
    factory.registerAlias("regexp", "rlike", FunctionFactory::CaseInsensitive);
}

}
