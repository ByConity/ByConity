#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiMatchAny
{
    static constexpr auto name = "multiMatchAny";
};

using FunctionMultiMatchAny = FunctionsMultiStringSearch<
    MultiMatchAnyImpl<UInt8, true, false, false>,
    NameMultiMatchAny,
    std::numeric_limits<UInt32>::max()>;

}

REGISTER_FUNCTION(MultiMatchAny)
{
    factory.registerFunction<FunctionMultiMatchAny>();
}

}
