#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "ExtractFirstSignificantSubdomain.h"


namespace DB
{

struct NameFirstSignificantSubdomain { static constexpr auto name = "firstSignificantSubdomain"; };
using FunctionFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<ExtractFirstSignificantSubdomain<true, false>>, NameFirstSignificantSubdomain>;

struct NameFirstSignificantSubdomainRFC { static constexpr auto name = "firstSignificantSubdomainRFC"; };
using FunctionFirstSignificantSubdomainRFC = FunctionStringToString<ExtractSubstringImpl<ExtractFirstSignificantSubdomain<true, true>>, NameFirstSignificantSubdomainRFC>;

REGISTER_FUNCTION(FirstSignificantSubdomain)
{
    factory.registerFunction<FunctionFirstSignificantSubdomain>(
        {
        R"(
Returns the "first significant subdomain".

The first significant subdomain is a second-level domain if it is 'com', 'net', 'org', or 'co'.
Otherwise, it is a third-level domain.

For example, firstSignificantSubdomain('https://news.clickhouse.com/') = 'clickhouse', firstSignificantSubdomain ('https://news.clickhouse.com.tr/') = 'clickhouse'.

The list of "insignificant" second-level domains and other implementation details may change in the future.
        )",
        Documentation::Examples{{"firstSignificantSubdomain", "SELECT firstSignificantSubdomain('https://news.clickhouse.com/')"}},
        Documentation::Categories{"URL"}
        });

    factory.registerFunction<FunctionFirstSignificantSubdomainRFC>(
        {
        R"(Returns the "first significant subdomain" according to RFC 1034.)",
        Documentation::Examples{},
        Documentation::Categories{"URL"}
        });
}

}
