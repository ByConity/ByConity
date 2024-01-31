#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/HasTokenImpl.h>

#include <Common/Volnitsky.h>

namespace DB
{

struct NameHasTokenCaseInsensitive
{
    static constexpr auto name = "hasTokenCaseInsensitive";
};

struct NameHasTokenCaseInsensitiveOrNull
{
    static constexpr auto name = "hasTokenCaseInsensitiveOrNull";
};

using FunctionHasTokenCaseInsensitive
    = FunctionsStringSearch<HasTokenImpl<NameHasTokenCaseInsensitive, VolnitskyCaseInsensitive, false>>;
using FunctionHasTokenCaseInsensitiveOrNull
    = FunctionsStringSearch<HasTokenImpl<NameHasTokenCaseInsensitiveOrNull, VolnitskyCaseInsensitive, false>, ExecutionErrorPolicy::Null>;

REGISTER_FUNCTION(HasTokenCaseInsensitive)
{
    factory.registerFunction<FunctionHasTokenCaseInsensitive>(DB::FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionHasTokenCaseInsensitiveOrNull>(DB::FunctionFactory::CaseInsensitive);
}

}
