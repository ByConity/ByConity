#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAnyCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchAnyCaseInsensitiveUTF8";
};

using FunctionMultiSearchCaseInsensitiveUTF8
    = FunctionsMultiStringSearch<MultiSearchImpl<PositionCaseInsensitiveUTF8>, NameMultiSearchAnyCaseInsensitiveUTF8>;

}

REGISTER_FUNCTION(MultiSearchAnyCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionMultiSearchCaseInsensitiveUTF8>();
}

}
