#pragma once
#include <Interpreters/Context.h>
#include <Optimizer/Rewriter/Rewriter.h>

namespace DB
{

/// A deadlock cte is caused by a join whose children share a same CTE,
/// see also https://******.
///
/// To solve this problem, we assign each step its execute_order, if a CTE is
/// referred by 2 steps which has different execute_order, then we consider it
/// is a deadlock cte and add BufferStes for each reference.
///
/// execute_order starts from the most direct right children of a plan tree,
/// initially 0. For each join, assign the execute_order of join node and left
/// nodes(including all descendant nodes) with (execute_order of right node + 1).
/// This process proceed bottom up until the plan root.
///
/// E.g.
///           A
///          / \
///         B   C    ,  execute_order: C = 0, A = B = 1
///
///           A
///          / \
///         B   C
///        / \
///       D   E      ,  execute_order: C = E = 0, A = B = D = 1
///
///           A
///          / \
///         B   C
///            / \
///           D   E  ,  execute_order: E = 0, D = C = 1, A = B = 2
///
///
/// Currently the algorithm will add buffer step for all reference of a deadlock cte.
/// i.e., given input plan:
///           Join
///          /     \
///     CTERef[0] CTERef[0]
/// the rewriter will output:
///           Join
///          /     \
///       Buffer  CTERef[0]
///         |
///     CTERef[0]
///
///
/// Currently the algorithm will add buffer step aggresively to solve cyclic deadlock ctes.
/// e.g.
///                  Union
///                    |
///            ------------------
///            |                |
///           Join             Join
///          /     \          /     \
///     CTERef[0] CTERef[1] CTERef[1] CTERef[0]
/// In some cases, this may cause some unnecessary buffer steps being created.
/// e.g.
///                  Union
///                    |
///            ------------------
///            |                |
///           Join             Join
///          /     \          /     \
///     CTERef[0] CTERef[1] CTERef[1] CTERef[2]
/// if cte_2 can execute quick enough.
class AddBufferForDeadlockCTE : public Rewriter
{
public:
    String name() const override { return "AddBufferForDeadlockCTE"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_buffer_for_deadlock_cte; }
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
};
}
