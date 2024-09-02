#pragma once

#include <Core/Names.h>
#include <Interpreters/AggregateDescription.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/Void.h>

namespace DB
{
/**
 * @class ExpressionReorderNormalizer can normalize an Expression in place, assuming symbols in the expr has been normalized
 *
 * Currently supports Names / NamesWithAliases / Assignments / AggregateDescriptions / AST with commutative functions.
 * Currently only and/or are considered commutative.
 */
class ExpressionReorderNormalizer : public ASTVisitor<size_t, Void>
{
public:
    static void reorder(Names & names);
    static void reorder(NamesWithAliases & name_alias);
    static void reorder(Assignments & assignments);
    static void reorder(AggregateDescriptions & descriptions);
    static void reorder(ColumnsWithTypeAndName & columnas);
    static size_t reorder(ASTPtr & ast);

protected:
    size_t visitNode(ASTPtr & ast, Void &) override;
    size_t visitASTFunction(ASTPtr & ast, Void &) override;
};

}
