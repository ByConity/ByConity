#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAny
{
    static constexpr auto name = "multiSearchAny";
};

using FunctionMultiSearch = FunctionsMultiStringSearch<MultiSearchImpl<PositionCaseSensitiveASCII>, NameMultiSearchAny>;

}

REGISTER_FUNCTION(MultiSearchAny)
{
    factory.registerFunction<FunctionMultiSearch>();
}

}
