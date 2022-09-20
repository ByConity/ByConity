#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
    void collectWhereClausePredicate(const ASTPtr & ast, std::map<String,String> & columns_to_values, const ContextPtr & context);
    std::vector<std::map<String,String>> collectWhereORClausePredicate(const ASTPtr & ast);
}
