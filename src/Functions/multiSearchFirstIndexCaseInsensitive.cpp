#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstIndexImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndexCaseInsensitive
{
    static constexpr auto name = "multiSearchFirstIndexCaseInsensitive";
};

using FunctionMultiSearchFirstIndexCaseInsensitive
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<PositionCaseInsensitiveASCII>, NameMultiSearchFirstIndexCaseInsensitive>;

}

REGISTER_FUNCTION(MultiSearchFirstIndexCaseInsensitive)
{
    factory.registerFunction<FunctionMultiSearchFirstIndexCaseInsensitive>();
}

}
