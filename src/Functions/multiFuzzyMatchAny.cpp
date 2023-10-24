#include "FunctionsMultiStringFuzzySearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiFuzzyMatchAny
{
    static constexpr auto name = "multiFuzzyMatchAny";
};

using FunctionMultiFuzzyMatchAny = FunctionsMultiStringFuzzySearch<
    MultiMatchAnyImpl<UInt8, true, false, true>,
    NameMultiFuzzyMatchAny,
    std::numeric_limits<UInt32>::max()>;

}

REGISTER_FUNCTION(MultiFuzzyMatchAny)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAny>();
}

}
