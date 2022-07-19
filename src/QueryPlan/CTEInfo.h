#pragma once

#include <Core/Types.h>

#include <memory>
#include <unordered_map>

namespace DB
{
using CTEId = UInt32;

class PlanNodeBase;
using PlanNodePtr = std::shared_ptr<PlanNodeBase>;

class CTEInfo
{
public:
    PlanNodePtr & getCTEDef(CTEId cte_id) { return common_table_expressions.at(cte_id); }

    bool contains(CTEId cte_id) const { return common_table_expressions.contains(cte_id); }

    std::unordered_map<CTEId, PlanNodePtr> & getCTEs() { return common_table_expressions; }

    void add(CTEId id, PlanNodePtr plan)
    {
        checkNotExists(id);
        common_table_expressions.emplace(id, std::move(plan));
    }

    void update(CTEId id, PlanNodePtr plan)
    {
        checkExists(id);
        common_table_expressions[id] = std::move(plan);
    }

    bool empty() const { return common_table_expressions.empty(); }

    size_t size() const { return common_table_expressions.size(); }

    std::unordered_map<CTEId, UInt64> collectCTEReferenceCounts(PlanNodePtr & root);

private:
    std::unordered_map<CTEId, PlanNodePtr> common_table_expressions;

    class ReferenceCountsVisitor;

    void checkNotExists(CTEId id) const;
    void checkExists(CTEId id) const;
};
}
