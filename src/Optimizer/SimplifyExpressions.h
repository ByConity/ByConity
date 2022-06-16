#pragma once

#include <Interpreters/Context.h>
#include <Parsers/ASTVisitor.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>

namespace DB
{
struct NodeContext
{
    enum Root
    {
        ROOT_NODE,
        NOT_ROOT_NODE
    };
    Root root;
    ContextMutablePtr & context;
};

/**
 * CommonPredicatesRewriter rewrite boolean function by following rules:
 *
 *   1. (A AND B) OR (A AND C) => A AND (B OR C);
 *   2. (A OR B) AND (A OR C) => A OR (B AND C);
 *   3. (A AND B) OR (C AND D) => (A OR C) AND (A OR D) AND (B OR C) AND (B OR D);
 *
 * Note that this rule is only used to convert the root predicate to a CNF.
 */
class CommonPredicatesRewriter : public ConstASTVisitor<ConstASTPtr, NodeContext>
{
public:
    static ConstASTPtr rewrite(const ConstASTPtr & predicate, ContextMutablePtr & context);
    ConstASTPtr visitNode(const ConstASTPtr &, NodeContext &) override;
    ConstASTPtr visitASTFunction(const ConstASTPtr &, NodeContext &) override;

private:
    ConstASTPtr process(const ConstASTPtr &, NodeContext &);
};

/**
 * SwapPredicateRewriter rewrite comparison expr like
 * `Literal ComparsionOp Identifier` to `Identifier FlipComparisonOp Literal`.
 */
class SwapPredicateRewriter : public ConstASTVisitor<ConstASTPtr, Void>
{
public:
    static ConstASTPtr rewrite(const ConstASTPtr & predicate, ContextMutablePtr & context);
    ConstASTPtr visitNode(const ConstASTPtr & node, Void & context) override;
    ConstASTPtr visitASTFunction(const ConstASTPtr & node, Void & context) override;
};

}
