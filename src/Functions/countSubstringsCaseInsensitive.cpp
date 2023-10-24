#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "CountSubstringsImpl.h"


namespace DB
{
namespace
{

struct NameCountSubstringsCaseInsensitive
{
    static constexpr auto name = "countSubstringsCaseInsensitive";
};

using FunctionCountSubstringsCaseInsensitive = FunctionsStringSearch<CountSubstringsImpl<PositionCaseInsensitiveASCII>, NameCountSubstringsCaseInsensitive>;

}

REGISTER_FUNCTION(CountSubstringsCaseInsensitive)
{
    factory.registerFunction<FunctionCountSubstringsCaseInsensitive>();
}
}
