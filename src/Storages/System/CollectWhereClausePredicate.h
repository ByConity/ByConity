#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
    std::vector<std::map<String,String>> collectWhereORClausePredicate(const ASTPtr & ast, const ContextPtr & context);
}
