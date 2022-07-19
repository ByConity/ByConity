#pragma once

#include <Optimizer/Rule/Rule.h>

namespace DB
{

class PushFilterIntoTableScan : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_FILTER_INTO_TABLE_SCAN; }
    String getName() const override { return "PUSH_FILTER_INTO_TABLE_SCAN"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;

private:
    static std::vector<ConstASTPtr> extractPushDownFilter(const std::vector<ConstASTPtr> & conjuncts, ContextMutablePtr & context);
    static std::vector<ConstASTPtr> removeStorageFilter(const std::vector<ConstASTPtr> & conjuncts);
};


class PushLimitIntoTableScan : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_LIMIT_INTO_TABLE_SCAN; }
    String getName() const override { return "PUSH_LIMIT_INTO_TABLE_SCAN"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
