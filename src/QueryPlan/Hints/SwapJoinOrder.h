#include <set>
#include <QueryPlan/Hints/IPlanHint.h>
#include <QueryPlan/Hints/PlanHintFactory.h>
#include <Parsers/IAST.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>


namespace DB
{

class SwapJoinOrder : public IPlanHint
{
public:
    static constexpr auto name = "SWAP_JOIN_ORDER";

    static PlanHintPtr create(const SqlHint & sql_hint, const ContextMutablePtr &)
    {
        if (sql_hint.getOptions().size() != 2 && sql_hint.getOptions()[0] != sql_hint.getOptions()[1])
            return {};
        return std::make_shared<SwapJoinOrder>(sql_hint.getOptions());
    }

    explicit SwapJoinOrder(const Strings & options_) : table_list(options_) {}

    String getName() const override { return name; }
    HintCategory getType() const override { return HintCategory::JOIN_ORDER; }
    Strings getOptions() const override { return table_list;}
    String getJoinOrderString() const;

    bool checkOptions(Strings & table_name_list) const;

    bool canAttach(PlanNodeBase & node, HintOptions & hint_options) const override;
private:

    Strings table_list;
};
}
