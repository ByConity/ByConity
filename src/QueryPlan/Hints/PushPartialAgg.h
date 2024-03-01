#pragma once
#include <Parsers/IAST.h>
#include <QueryPlan/Hints/IPlanHint.h>
#include <QueryPlan/Hints/PlanHintFactory.h>
#include <QueryPlan/PlanNode.h>


namespace DB
{

class PushPartialAgg : public IPlanHint
{
public:
    explicit PushPartialAgg(const Strings & options_) : options(options_)
    {
    }

    HintCategory getType() const override
    {
        return HintCategory::PUSH_PARTIAL_AGG;
    }
    String & getFunctionName()
    {
        return options.at(0);
    }
    Strings getOptions() const override
    {
        return options;
    }

    bool canAttach(PlanNodeBase & node, HintOptions & hint_options) const override;

protected:
    Strings options;
};

class EnablePushPartialAgg : public PushPartialAgg
{
public:
    static constexpr auto name = "Enable_Push_Partial_Agg";

    static PlanHintPtr create(const SqlHint & sql_hint, const ContextMutablePtr & context)
    {
        if (sql_hint.getOptions().size() != 1)
            return {};
        return std::make_shared<EnablePushPartialAgg>(sql_hint, context);
    }

    EnablePushPartialAgg(const SqlHint & sql_hint, const ContextMutablePtr &) : PushPartialAgg(sql_hint.getOptions())
    {
    }

    String getName() const override
    {
        return name;
    }
};


class DisablePushPartialAgg : public PushPartialAgg
{
public:
    static constexpr auto name = "Disable_Push_Partial_Agg";

    static PlanHintPtr create(const SqlHint & sql_hint, const ContextMutablePtr & context)
    {
        if (sql_hint.getOptions().size() != 1)
            return {};
        return std::make_shared<DisablePushPartialAgg>(sql_hint, context);
    }

    DisablePushPartialAgg(const SqlHint & sql_hint, const ContextMutablePtr &) : PushPartialAgg(sql_hint.getOptions())
    {
    }

    String getName() const override
    {
        return name;
    }
};
}
