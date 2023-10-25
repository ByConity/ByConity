#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "CountSubstringsImpl.h"


namespace DB
{
namespace
{

struct NameCountSubstringsCaseInsensitiveUTF8
{
    static constexpr auto name = "countSubstringsCaseInsensitiveUTF8";
};

using FunctionCountSubstringsCaseInsensitiveUTF8 = FunctionsStringSearch<CountSubstringsImpl<PositionCaseInsensitiveUTF8>, NameCountSubstringsCaseInsensitiveUTF8>;

}

REGISTER_FUNCTION(CountSubstringsCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionCountSubstringsCaseInsensitiveUTF8>();
}
}
