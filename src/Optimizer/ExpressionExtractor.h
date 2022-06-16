#pragma once

#include <Parsers/ASTVisitor.h>
#include <QueryPlan/PlanVisitor.h>
#include <Optimizer/Utils.h>

namespace DB
{
using ConstASTSet = std::unordered_set<ConstASTPtr, Utils::ConstASTHash, Utils::ConstASTEquals>;

class ExpressionExtractor
{
public:
    static std::vector<ConstASTPtr> extract(PlanNodePtr & node);
};

class ExpressionVisitor : public PlanNodeVisitor<Void, std::vector<ConstASTPtr>>
{
public:
    Void visitPlanNode(PlanNodeBase &, std::vector<ConstASTPtr> & expressions) override;
    Void visitProjectionNode(ProjectionNode &, std::vector<ConstASTPtr> & expressions) override;
    Void visitFilterNode(FilterNode &, std::vector<ConstASTPtr> & expressions) override;
    Void visitAggregatingNode(AggregatingNode &, std::vector<ConstASTPtr> & expressions) override;
    Void visitApplyNode(ApplyNode &, std::vector<ConstASTPtr> & expressions) override;
    Void visitJoinNode(JoinNode &, std::vector<ConstASTPtr> & expressions) override;
};

class SubExpressionExtractor
{
public:
    static ConstASTSet extract(ConstASTPtr node);
};

class SubExpressionVisitor : public ConstASTVisitor<Void, ConstASTSet>
{
public:
    Void visitNode(const ConstASTPtr &, ConstASTSet & context) override;
    Void visitASTFunction(const ConstASTPtr &, ConstASTSet & context) override;
    Void visitASTIdentifier(const ConstASTPtr &, ConstASTSet & context) override;
};

}
