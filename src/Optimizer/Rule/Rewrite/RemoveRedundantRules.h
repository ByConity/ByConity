#pragma once
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rule.h>

namespace DB
{
class RemoveRedundantFilter : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_FILTER; }
    String getName() const override { return "REMOVE_REDUNDANT_FILTER"; }

    PatternPtr getPattern() const override { return Patterns::filter(); }


protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantUnion : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_UNION; }
    String getName() const override { return "REMOVE_REDUNDANT_UNION"; }

    PatternPtr getPattern() const override { return Patterns::unionn(); }


protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantProjection : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_PROJECTION; }
    String getName() const override { return "REMOVE_REDUNDANT_PROJECTION"; }

    PatternPtr getPattern() const override { return Patterns::project(); }

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantEnforceSingleRow : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_ENFORCE_SINGLE_ROW; }
    String getName() const override { return "REMOVE_REDUNDANT_ENFORCE_SINGLE_ROW"; }

    PatternPtr getPattern() const override { return Patterns::enforceSingleRow(); }

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantCrossJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_CROSS_JOIN; }
    String getName() const override { return "REMOVE_REDUNDANT_CROSS_JOIN"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveReadNothing: public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_READ_NOTHING; }
    String getName() const override { return "REMOVE_READ_NOTHING"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_JOIN; }
    String getName() const override { return "REMOVE_REDUNDANT_JOIN"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class RemoveRedundantOuterJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_OUTER_JOIN; }
    String getName() const override { return "REMOVE_REDUNDANT_JOIN"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};


class RemoveRedundantLimit : public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_LIMIT; }
    String getName() const override { return "REMOVE_REDUNDANT_LIMIT"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

// TODO group by columns is distinct, no aggregate functions.
class RemoveRedundantAggregate: public Rule
{
public:
    RuleType getType() const override { return RuleType::REMOVE_REDUNDANT_AGGREGATE; }
    String getName() const override { return "REMOVE_REDUNDANT_AGGREGATE"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
