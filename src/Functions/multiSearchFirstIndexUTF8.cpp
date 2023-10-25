#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstIndexImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndexUTF8
{
    static constexpr auto name = "multiSearchFirstIndexUTF8";
};

using FunctionMultiSearchFirstIndexUTF8
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<PositionCaseSensitiveUTF8>, NameMultiSearchFirstIndexUTF8>;

}

REGISTER_FUNCTION(MultiSearchFirstIndexUTF8)
{
    factory.registerFunction<FunctionMultiSearchFirstIndexUTF8>();
}

}
