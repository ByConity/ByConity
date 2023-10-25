#include "FunctionsMultiStringPosition.h"
#include "FunctionFactory.h"
#include "MultiSearchAllPositionsImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAllPositionsCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchAllPositionsCaseInsensitiveUTF8";
};

using FunctionMultiSearchAllPositionsCaseInsensitiveUTF8 = FunctionsMultiStringPosition<
    MultiSearchAllPositionsImpl<PositionCaseInsensitiveUTF8>,
    NameMultiSearchAllPositionsCaseInsensitiveUTF8>;

}

REGISTER_FUNCTION(MultiSearchAllPositionsCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionMultiSearchAllPositionsCaseInsensitiveUTF8>();
}

}
