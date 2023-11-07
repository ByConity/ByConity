#pragma once
#include <Parsers/IAST.h>
#include <QueryPlan/Hints/IPlanHint.h>
#include <QueryPlan/Hints/JoinAlgorithmHints.h>
#include <QueryPlan/Hints/PlanHintFactory.h>
#include <QueryPlan/PlanNode.h>


namespace DB
{

class UseGraceHash : public JoinAlgorithmHints
{
public:
    static constexpr auto name = "USE_GRACE_HASH";

    static PlanHintPtr create(const SqlHint & sql_hint, const ContextMutablePtr & context)
    {
        if (sql_hint.getOptions().size() < 2)
            return {};
        return std::make_shared<UseGraceHash>(sql_hint, context);
    }

    UseGraceHash(const SqlHint & sql_hint, const ContextMutablePtr &);

    String getName() const override;
};

}
