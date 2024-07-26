#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>

namespace DB
{
    String queryToString(const ASTPtr & query, bool always_quote_identifiers)
    {
        return queryToString(*query, always_quote_identifiers);
    }

    String queryToString(const IAST & query, bool always_quote_identifiers)
    {
        return serializeAST(query, true, always_quote_identifiers);
    }
}
