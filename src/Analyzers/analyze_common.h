#pragma once

#include <Analyzers/Analysis.h>
#include <Interpreters/asof.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
/// join
ASTPtr joinCondition(const ASTTableJoin & join);
bool isNormalInnerJoin(const ASTTableJoin & join);
bool isCrossJoin(const ASTTableJoin & join);
bool isSemiOrAntiJoin(const ASTTableJoin & join);
bool isAsofJoin(const ASTTableJoin & join);
String getFunctionForInequality(ASOF::Inequality inequality);

/// expressions
std::vector<ASTPtr> expressionToCnf(const ASTPtr & node);
ASTPtr cnfToExpression(const std::vector<ASTPtr> & cnf);

std::vector<ASTPtr> extractExpressions(ContextPtr context, Analysis & analysis, ASTPtr root, bool include_subquery = false,
    const std::function<bool(const ASTPtr &)> & filter = [](const auto &) {return true;});

std::vector<ASTPtr> extractReferencesToScope(ContextPtr context, Analysis & analysis, ASTPtr root, ScopePtr scope, bool include_subquery = false);

}
