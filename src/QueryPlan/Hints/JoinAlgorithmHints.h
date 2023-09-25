#pragma once
#include <Parsers/IAST.h>
#include <QueryPlan/Hints/IPlanHint.h>
#include <QueryPlan/Hints/PlanHintFactory.h>
#include <QueryPlan/PlanNode.h>


namespace DB
{
enum class JoinAlgorithmType
{
    UNKNOWN = 0,
    USE_GRACE_HASH,
};

class JoinAlgorithmHints : public IPlanHint
{
public:
    explicit JoinAlgorithmHints(const Strings & options_);

    HintCategory getType() const override;
    Strings getOptions() const override;

    bool checkOptions(Strings & table_name_list) const;
    bool canAttach(PlanNodeBase & node, HintOptions & hint_options) const override;

protected:
    Strings options;
};

}
