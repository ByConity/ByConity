#include <QueryPlan/PlanNode.h>

#include <utility>

namespace DB
{

#define PLAN_NODE_DEF(TYPE) \
template class PlanNode<TYPE##Step>;
APPLY_STEP_TYPES(PLAN_NODE_DEF)
PLAN_NODE_DEF(Any)
#undef PLAN_NODE_DEF

}
