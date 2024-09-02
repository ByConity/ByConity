#pragma once

#include <Parsers/IAST.h>

namespace DB
{
    String queryToString(const ASTPtr & query, bool always_quote_identifiers = false);
    String queryToString(const IAST & query, bool always_quote_identifiers = false);
}
