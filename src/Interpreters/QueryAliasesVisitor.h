#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTSelectQuery;
class ASTSubquery;
struct ASTTableExpression;
struct ASTArrayJoin;

template <bool allow_ambiguous_>
struct QueryAliasesWithSubqueries
{
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
    static constexpr bool allow_ambiguous = allow_ambiguous_;
};

template <bool allow_ambiguous_>
struct QueryAliasesNoSubqueries
{
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
    static constexpr bool allow_ambiguous = allow_ambiguous_;
};

/// Visits AST node to collect aliases.
template <typename Helper>
class QueryAliasesMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<QueryAliasesMatcher, false>;

    using Data = Aliases;

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child) { return Helper::needChildVisit(node, child); }

private:
    static void visit(const ASTSelectQuery & select, const ASTPtr & ast, Data & data);
    static void visit(const ASTSubquery & subquery, const ASTPtr & ast, Data & data);
    static void visit(const ASTArrayJoin &, const ASTPtr & ast, Data & data);
    static void visitOther(const ASTPtr & ast, Data & data);
};

/// Visits AST nodes and collect their aliases in one map (with links to source nodes).
using QueryAliasesVisitor = QueryAliasesMatcher<QueryAliasesWithSubqueries<false>>::Visitor;
using QueryAliasesNoSubqueriesVisitor = QueryAliasesMatcher<QueryAliasesNoSubqueries<false>>::Visitor;

using QueryAliasesAllowAmbiguousVisitor = QueryAliasesMatcher<QueryAliasesWithSubqueries<true>>::Visitor;
using QueryAliasesAllowAmbiguousNoSubqueriesVisitor = QueryAliasesMatcher<QueryAliasesNoSubqueries<true>>::Visitor;
}
