#pragma once
#include <vector>
#include <Optimizer/Rule/Rule.h>

namespace DB
{
class Rules
{
public:
    static std::vector<RulePtr> mergeSetRules();
    static std::vector<RulePtr> implementSetRules();
    static std::vector<RulePtr> normalizeExpressionRules();
    static std::vector<RulePtr> simplifyExpressionRules();
    static std::vector<RulePtr> inlineProjectionRules();
    static std::vector<RulePtr> pushPartialStepRules();
    static std::vector<RulePtr> pushAggRules();
    static std::vector<RulePtr> pushDownLimitRules();
    static std::vector<RulePtr> removeRedundantRules();
    static std::vector<RulePtr> distinctToAggregateRules();
    static std::vector<RulePtr> pushIntoTableScanRules();
};

}
