#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTSelectWithUnionQuery;
class ASTSubquery;
class ASTSelectQuery;
struct ASTTableExpression;
struct ASTArrayJoin;

/// Visits AST node to collect alias --> vector of original column names.
class MultipleAliasMatcher
{
public:
    using Visitor = InDepthNodeVisitor<MultipleAliasMatcher, false>;

    struct Data
    {
        MultipleAliases & multiple_aliases;
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);

private:
    static void visit(ASTSubquery & subquery, const ASTPtr & ast, Data & data);
    static void visit(const ASTArrayJoin &, const ASTPtr & ast, Data & data);
    static void visitOther(const ASTPtr & ast, Data & data);
    static void visitFilter(ASTPtr & filter);
    static void visit(ASTSelectQuery & select, const ASTPtr & ast, Data & data);
};

/// Visits AST nodes and collect their aliases in one map (with links to source nodes).
using MultipleAliasVisitor = MultipleAliasMatcher::Visitor;

}
