#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/Iterative/IterativeRewriter.h>
#include <Optimizer/Rewriter/AddBufferForDeadlockCTE.h>
#include <Optimizer/Rewriter/ColumnPruning.h>
#include <Optimizer/Rewriter/PredicatePushdown.h>
#include <Optimizer/Rewriter/RemoveUnusedCTE.h>
#include <Optimizer/Rule/Rewrite/PushDownLimitRules.h>
#include <Optimizer/Rewriter/UnifyNullableType.h>
#include <Optimizer/Rule/Rules.h>
#include <QueryPlan/BufferStep.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SimplePlanVisitor.h>
#include <QueryPlan/Void.h>
#include <fmt/core.h>
#include <common/logger_useful.h>

#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace
{
    // visitor to find direct join right node of a query plan and collect deadlock ctes
    class FindDirectRightVisitor;
    // visitor to update execute_order from a direct join right to plan root
    class UpdateParentExecuteOrderVisitor;
    // visitor to update execute_order from a join node to left descendant node
    class UpdateLeftExecuteOrderVisitor;

    // visitor to find cte ref exists both sides of join build and probe to add buffer
    class FindAllCTEIfExistsOnJoinBuildVisitor;

    // visitor to add BufferStep for deadlock CTEs
    class AddBufferVisitor;

    enum class JoinPath
    {
        LEFT,
        RIGHT
    };

    using VisitEntry = std::pair<PlanNodeBase *, JoinPath>;
    using VisitPath = std::vector<VisitEntry>;
    struct CTEExecuteOrder
    {
        PlanNodeId node_id;
        CTEId cte_id;
        int execute_order;
    };

    using ExecuteOrders = std::vector<CTEExecuteOrder>;

    class FindDirectRightVisitor : public PlanNodeVisitor<void, const JoinPath>
    {
    public:
        explicit FindDirectRightVisitor(CTEInfo & cte_info_, LoggerPtr logger_) : cte_info(cte_info_), logger(logger_)
        {
        }

        void visitPlanNode(PlanNodeBase & node, const JoinPath &) override;
        void visitCTERefNode(CTERefNode & node, const JoinPath &) override;
        void visitJoinNode(JoinNode & node, const JoinPath &) override;

        CTEInfo & cte_info;
        LoggerPtr logger;
        std::unordered_set<PlanNodeId> deadlock_ctes;
        VisitPath visit_path;
    };

    class UpdateParentExecuteOrderVisitor : public PlanNodeVisitor<void, const Void>
    {
    public:
        UpdateParentExecuteOrderVisitor(CTEInfo & cte_info_, VisitPath visit_path_)
            : cte_info(cte_info_), visit_path(std::move(visit_path_))
        {
        }

        void visitPlanNode(PlanNodeBase & node, const Void &) override;
        void visitJoinNode(JoinNode & node, const Void &) override;

        CTEInfo & cte_info;
        VisitPath visit_path;
        ExecuteOrders execute_orders;
        int cur_execute_order = 0;
    };

    class UpdateLeftExecuteOrderVisitor : public PlanNodeVisitor<void, const Void>
    {
    public:
        UpdateLeftExecuteOrderVisitor(CTEInfo & cte_info_, ExecuteOrders & execute_orders_, int execute_order_)
            : cte_info(cte_info_), execute_orders(execute_orders_), execute_order(execute_order_)
        {
        }

        void visitPlanNode(PlanNodeBase & node, const Void &) override;
        void visitCTERefNode(CTERefNode & node, const Void &) override;
        void visitJoinNode(JoinNode & node, const Void &) override;

        CTEInfo & cte_info;
        ExecuteOrders & execute_orders;
        const int execute_order = 0;
    };

    class AddBufferVisitor : public SimplePlanRewriter<const Void>
    {
    public:
        AddBufferVisitor(
            const std::unordered_set<PlanNodeId> & deadlock_ctes_, ContextMutablePtr context_, CTEInfo & cte_info_, LoggerPtr logger_)
            : SimplePlanRewriter<const Void>(std::move(context_), cte_info_), deadlock_ctes(deadlock_ctes_), logger(logger_)
        {
        }

        PlanNodePtr visitCTERefNode(CTERefNode & node, const Void & c) override;

        const std::unordered_set<PlanNodeId> & deadlock_ctes;
        LoggerPtr logger;
    };

    void FindDirectRightVisitor::visitPlanNode(PlanNodeBase & node, const JoinPath & join_path)
    {
        visit_path.emplace_back(&node, join_path);

        if (node.getChildren().empty() && join_path == JoinPath::RIGHT)
        {
            UpdateParentExecuteOrderVisitor update_parent_order_visitor{cte_info, visit_path};
            VisitorUtil::accept(node, update_parent_order_visitor, {});
            const auto & execute_orders = update_parent_order_visitor.execute_orders;

            LOG_TRACE(logger, "FindDirectRightVisitor visit on node {}", node.getId());

            std::unordered_map<CTEId, int> cte_min_execute_orders;
            for (const auto & execute_order : execute_orders)
            {
                auto it = cte_min_execute_orders.find(execute_order.cte_id);
                if (it == cte_min_execute_orders.end())
                    cte_min_execute_orders.emplace(execute_order.cte_id, execute_order.execute_order);
                else
                    it->second = std::min(it->second, execute_order.execute_order);
            }

            // find deadlock ctes
            for (const auto & execute_order : execute_orders)
            {
                LOG_TRACE(
                    logger,
                    "Direct right node id: {}, cte_id: {}, execute order: {}, cte min execute order: {}",
                    execute_order.node_id,
                    execute_order.cte_id,
                    execute_order.execute_order,
                    cte_min_execute_orders[execute_order.cte_id]);

                if (execute_order.execute_order > cte_min_execute_orders[execute_order.cte_id])
                    deadlock_ctes.emplace(execute_order.node_id);
            }
        }

        for (auto & child : node.getChildren())
            VisitorUtil::accept(*child, *this, join_path);

        visit_path.pop_back();
    }

    void FindDirectRightVisitor::visitCTERefNode(CTERefNode & node, const JoinPath & join_path)
    {
        visit_path.emplace_back(&node, join_path);

        auto cte_id = node.getStep()->getId();
        VisitorUtil::accept(*cte_info.getCTEDef(cte_id), *this, join_path);

        visit_path.pop_back();
    }

    void FindDirectRightVisitor::visitJoinNode(JoinNode & node, const JoinPath & join_path)
    {
        visit_path.emplace_back(&node, join_path);

        assert(node.getChildren().size() == 2);
        VisitorUtil::accept<void, const JoinPath>(*node.getChildren().at(0), *this, JoinPath::LEFT);
        VisitorUtil::accept<void, const JoinPath>(*node.getChildren().at(1), *this, JoinPath::RIGHT);

        visit_path.pop_back();
    }

    void UpdateParentExecuteOrderVisitor::visitPlanNode(PlanNodeBase & node, const Void & ctx)
    {
        assert(visit_path.back().second == JoinPath::RIGHT);
        visit_path.pop_back();

        if (!visit_path.empty())
        {
            // for non-root node, visit its parent
            VisitorUtil::accept(*visit_path.back().first, *this, ctx);
        }
        else
        {
            // for root node, update execute_order for left tree of root node.
            // also we assume the root node can not be a CTERef node.
            UpdateLeftExecuteOrderVisitor update_left_order_visitor_for_union{cte_info, execute_orders, cur_execute_order};
            for (auto & child : node.getChildren())
                VisitorUtil::accept(*child, update_left_order_visitor_for_union, {});
        }
    }

    void UpdateParentExecuteOrderVisitor::visitJoinNode(JoinNode & node, const Void & ctx)
    {
        auto join_path = visit_path.back().second;
        visit_path.pop_back();

        ++cur_execute_order;

        // update execute_order for left tree of join node
        UpdateLeftExecuteOrderVisitor update_left_order_visitor{cte_info, execute_orders, cur_execute_order};
        VisitorUtil::accept(*node.getChildren().at(0), update_left_order_visitor, {});

        UpdateLeftExecuteOrderVisitor update_left_order_visitor_for_union{cte_info, execute_orders, cur_execute_order - 1};
        VisitorUtil::accept(*node.getChildren().at(1), update_left_order_visitor_for_union, {});

        if (!visit_path.empty() && join_path == JoinPath::RIGHT)
            VisitorUtil::accept(*visit_path.back().first, *this, ctx);
    }

    void UpdateLeftExecuteOrderVisitor::visitPlanNode(PlanNodeBase & node, const Void & ctx)
    {

        for (auto & child : node.getChildren())
            VisitorUtil::accept(*child, *this, ctx);
    }

    void UpdateLeftExecuteOrderVisitor::visitCTERefNode(CTERefNode & node, const Void & ctx)
    {

        auto cte_id = node.getStep()->getId();
        execute_orders.emplace_back(CTEExecuteOrder{node.getId(), cte_id, execute_order});
        VisitorUtil::accept(*cte_info.getCTEDef(cte_id), *this, ctx);
    }

    void UpdateLeftExecuteOrderVisitor::visitJoinNode(JoinNode & node, const Void & ctx)
    {
        VisitorUtil::accept(*node.getChildren().at(0), *this, ctx);
    }

    PlanNodePtr AddBufferVisitor::visitCTERefNode(CTERefNode & node, const Void & c)
    {
        SimplePlanRewriter<const Void>::visitCTERefNode(node, c);
        auto cte_id = node.getStep()->getId();

        if (!deadlock_ctes.count(node.getId()))
            return node.shared_from_this();

        /**
         * if buffer size exceed max_buffer_size_for_deadlock_cte, we inline cte instead of add buffer.
         * 
         * note: max buffer size for tpcds 1t is 7994883314, so we set max_buffer_size_for_deadlock_cte 
         * 8'000'000'000 bytes (8Gb) by default for tpcds 1T
         */
        Int64 max_buffer_size = context->getSettingsRef().max_buffer_size_for_deadlock_cte;
        if (max_buffer_size == 0)
        {
            LOG_TRACE(logger, "Inline CTE {} because max_buffer_size_for_deadlock_cte=0", cte_id);
            return node.getStep()->toInlinedPlanNode(cte_helper.getCTEInfo(), context);
        }

        if (max_buffer_size > 0)
        {
            auto stats = CardinalityEstimator::estimate(node, cte_helper.getCTEInfo(), context);
            if (!stats)
            {
                LOG_TRACE(logger, "Inline CTE {} because estimates stats failed", cte_id);
                return node.getStep()->toInlinedPlanNode(cte_helper.getCTEInfo(), context);
            }

            Int64 buffer_size = (*stats)->getOutputSizeInBytes();
            LOG_TRACE(logger, "CTE {} estimated buffer size {}", cte_id, (*stats)->getOutputSizeInBytes());
            if (buffer_size > max_buffer_size)
            {
                LOG_TRACE(
                    logger,
                    "Inline CTE {} because estimates buffer size {} is bigger than max_buffer_size_for_deadlock_cte({})",
                    cte_id,
                    buffer_size,
                    max_buffer_size);
                return node.getStep()->toInlinedPlanNode(cte_helper.getCTEInfo(), context);
            }
        }

        QueryPlanStepPtr buffer_step = std::make_shared<BufferStep>(node.getCurrentDataStream());
        PlanNodePtr buffer_node
            = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(buffer_step), {node.shared_from_this()}, node.getStatistics());
        return buffer_node;
    }

    class FindAllCTEIfExistsOnJoinBuildVisitor : public PlanNodeVisitor<std::unordered_set<CTEId>, Void>
    {
    public:
        explicit FindAllCTEIfExistsOnJoinBuildVisitor(CTEInfo & cte_info) : cte_helper(cte_info)
        {
        }

        std::unordered_set<CTEId> visitPlanNode(PlanNodeBase & node, Void & c) override
        {
            std::unordered_set<PlanNodeId> ctes;
            for (const auto & child : node.getChildren())
            {
                auto child_ctes = VisitorUtil::accept(*child, *this, c);
                ctes.insert(child_ctes.begin(), child_ctes.end());
            }
            return ctes;
        }

        std::unordered_set<CTEId> visitCTERefNode(CTERefNode & node, Void & c) override
        {
            const auto * cte_step = dynamic_cast<const CTERefStep *>(node.getStep().get());
            auto cte_id = cte_step->getId();
            cte_refs[cte_id].emplace_back(node.getId());

            auto ctes = cte_helper.accept(cte_id, *this, c);
            ctes.emplace(cte_id);

            return ctes;
        }

        std::unordered_set<CTEId> visitJoinNode(JoinNode & node, Void & c) override
        {
            auto left_ctes = VisitorUtil::accept(*node.getChildren()[0], *this, c);
            auto right_ctes = VisitorUtil::accept(*node.getChildren()[1], *this, c);
            for (const auto & cte_id : right_ctes)
            {
                for (const auto & node_id : cte_refs[cte_id])
                    deadlock_ctes.emplace(node_id);
            }

            left_ctes.insert(right_ctes.begin(), right_ctes.end());
            return left_ctes;
        }

        SimpleCTEVisitHelper<std::unordered_set<PlanNodeId>> cte_helper;
        std::unordered_map<CTEId, std::vector<PlanNodeId>> cte_refs;

        std::unordered_set<PlanNodeId> deadlock_ctes;
    };
}

bool AddBufferForDeadlockCTE::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    static auto logger = getLogger("AddBufferForDeadlockCTE");

    if (plan.getCTEInfo().empty())
        return false;

    std::unordered_set<PlanNodeId> deadlock_ctes;

    // fixme: fix deadlock algorithm to enable this settings
    if (context->getSettings().enable_remove_remove_unnecessary_buffer)
    {
        FindDirectRightVisitor find_deadlock_cte_visitor{plan.getCTEInfo(), logger};
        VisitorUtil::accept<void, const JoinPath>(plan.getPlanNode(), find_deadlock_cte_visitor, JoinPath::RIGHT);
        deadlock_ctes = std::move(find_deadlock_cte_visitor.deadlock_ctes);
    }
    else
    {
        FindAllCTEIfExistsOnJoinBuildVisitor find_all_cte_ref_visitor{plan.getCTEInfo()};
        Void c;
        VisitorUtil::accept(plan.getPlanNode(), find_all_cte_ref_visitor, c);
        deadlock_ctes = std::move(find_all_cte_ref_visitor.deadlock_ctes);
    }

    if (deadlock_ctes.empty())
        return false;

    if (logger->debug())
    {
        std::ostringstream os;
        for (const auto & cte_ref_id : deadlock_ctes)
            os << cte_ref_id << ", ";
        LOG_DEBUG(logger, "Detected deadlock ctes(cte_ref_id): {}", os.str());
    }

    AddBufferVisitor add_buffer_visitor{deadlock_ctes, context, plan.getCTEInfo(), logger};
    plan.update(VisitorUtil::accept(plan.getPlanNode(), add_buffer_visitor, {}));

    RewriterPtr push_limit_through_buffer = std::make_shared<IterativeRewriter>(
        std::vector<RulePtr>{std::make_shared<PushLimitThroughBuffer>()}, "PushDownLimitThroughBuffer");
    push_limit_through_buffer->rewritePlan(plan, context);

    if (context->getSettingsRef().max_buffer_size_for_deadlock_cte >= 0)
    {
        static Rewriters rewriters
            = {std::make_shared<RemoveUnusedCTE>(),
               std::make_shared<ColumnPruning>(),
               std::make_shared<PredicatePushdown>(false, true),
               std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),
               std::make_shared<UnifyNullableType>(),
               std::make_shared<IterativeRewriter>(Rules::normalizeExpressionRules(), "NormalizeExpression"),
               std::make_shared<IterativeRewriter>(Rules::swapPredicateRules(), "SwapPredicate"),
               std::make_shared<IterativeRewriter>(Rules::simplifyExpressionRules(), "SimplifyExpression"),
               std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant")};

        for (auto & rewriter : rewriters)
            rewriter->rewritePlan(plan, context);
    }
    return true;
}

}
