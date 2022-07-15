#include <QueryPlan/CTEInfo.h>

#include <QueryPlan/CTEVisitHelper.h>

namespace DB
{
class CTEInfo::ReferenceCountsVisitor : public PlanNodeVisitor<Void, std::unordered_map<CTEId, UInt64>>
{
public:
    explicit ReferenceCountsVisitor(CTEInfo & cte_info_) : cte_helper(cte_info_) { }

    Void visitPlanNode(PlanNodeBase & node, std::unordered_map<CTEId, UInt64> & c) override
    {
        for (auto & child : node.getChildren())
            VisitorUtil::accept(*child, *this, c);
        return Void{};
    }

    Void visitCTERefNode(CTERefNode & node, std::unordered_map<CTEId, UInt64> & reference_counts) override {
        const auto * cte_step = dynamic_cast<const CTERefStep *>(node.getStep().get());
        auto cte_id = cte_step->getId();
        ++reference_counts[cte_id];
        cte_helper.accept(cte_id, *this, reference_counts);
        return Void{};
    }
private:
    CTEPreorderVisitHelper cte_helper;
};

std::unordered_map<CTEId, UInt64> CTEInfo::collectCTEReferenceCounts(PlanNodePtr & root)
{
    ReferenceCountsVisitor visitor {*this};
    std::unordered_map<CTEId, UInt64> reference_counts;
    VisitorUtil::accept(*root, visitor, reference_counts);
    return reference_counts;
}

void CTEInfo::checkNotExists(CTEId id) const
{
    if (contains(id))
        throw Exception("CTE " + std::to_string(id) + " already exists", ErrorCodes::LOGICAL_ERROR);
}

void CTEInfo::checkExists(CTEId id) const
{
    if (!contains(id))
        throw Exception("CTE " + std::to_string(id) + " don't exists", ErrorCodes::LOGICAL_ERROR);
}
}

