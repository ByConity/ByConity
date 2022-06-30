#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
    void collectWhereClausePredicate(const ASTPtr & ast, std::map<String,String> & columnToValue);
    std::vector<std::map<String,String>> collectWhereORClausePredicate(const ASTPtr & ast);
}
