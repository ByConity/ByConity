#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiMatchAnyIndex
{
    static constexpr auto name = "multiMatchAnyIndex";
};

using FunctionMultiMatchAnyIndex = FunctionsMultiStringSearch<
    MultiMatchAnyImpl<UInt64, false, true, false>,
    NameMultiMatchAnyIndex,
    std::numeric_limits<UInt32>::max()>;

}

REGISTER_FUNCTION(MultiMatchAnyIndex)
{
    factory.registerFunction<FunctionMultiMatchAnyIndex>();
}

}
