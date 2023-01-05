#include <QueryPlan/PlanNode.h>
namespace DB
{
std::pair<String, PlanNodePtr> createDummyPlanNode(ContextMutablePtr context);

} // namespace DB
