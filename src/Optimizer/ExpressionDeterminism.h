#pragma once

#include <Interpreters/Context.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST.h>
#include <QueryPlan/Assignment.h>

namespace DB
{
class ExpressionDeterminism
{
public:
    static std::set<String> getDeterministicSymbols(Assignments & assignments, ContextMutablePtr & context);
    static ConstASTPtr filterDeterministicConjuncts(ConstASTPtr predicate, ContextMutablePtr & context);
    static ConstASTPtr filterNonDeterministicConjuncts(ConstASTPtr predicate, ContextMutablePtr & context);
    static std::set<ConstASTPtr> filterDeterministicPredicates(std::vector<ConstASTPtr> & predicates, ContextMutablePtr & context);
    static bool isDeterministic(ConstASTPtr expression, ContextMutablePtr & context);
};

class DeterminismVisitor : public ConstASTVisitor<Void, ContextMutablePtr>
{
public:
    explicit DeterminismVisitor(bool isDeterministic);
    Void visitNode(const ConstASTPtr & node, ContextMutablePtr & context) override;
    Void visitASTFunction(const ConstASTPtr & node, ContextMutablePtr & context) override;
    bool isDeterministic() const { return is_deterministic; }

private:
    bool is_deterministic;
};

}
