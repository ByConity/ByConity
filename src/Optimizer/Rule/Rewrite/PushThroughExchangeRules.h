//#pragma once
//#include <Optimizer/Rule/Rule.h>
//
//namespace DB
//{
//class PushProjectionThroughExchange : public Rule
//{
//public:
//    RuleType getType() const override { return RuleType::PUSH_PROJECTION_THROUGH_EXCHANGE; }
//    String getName() const override { return "PUSH_PROJECTION_THROUGH_EXCHANGE"; }
//
//    PatternPtr getPattern() const override;
//
//    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
//};
//}
