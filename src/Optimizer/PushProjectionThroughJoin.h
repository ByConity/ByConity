#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB
{

/**
 * Pushing projections through inner join so that joins are not separated
 * by a project node and can participate in cross join elimination or join reordering.
 */
class PushProjectionThroughJoin
{
public:
    static std::optional<PlanNodePtr> pushProjectionThroughJoin(ProjectionNode & project, ContextMutablePtr & context);
    static PlanNodePtr inlineProjections(PlanNodePtr parent_projection, ContextMutablePtr & context);
    static std::set<String> getJoinRequiredSymbols(JoinNode & node);
};

}
