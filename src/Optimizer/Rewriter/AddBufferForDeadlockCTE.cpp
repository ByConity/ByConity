#include <Optimizer/Rewriter/AddBufferForDeadlockCTE.h>
#include <QueryPlan/BufferStep.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>
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
    // visitor to add BufferStep for deadlock CTEs
    class AddBufferVisitor;

    enum class JoinPath
    {
        LEFT,
        RIGHT
    };

    using VisitEntry = std::pair<PlanNodeBase *, JoinPath>;
    using VisitPath = std::vector<VisitEntry>;
    using ExecuteOrderMap = std::unordered_map<PlanNodeId, std::unordered_set<int>>;

    class FindDirectRightVisitor : public PlanNodeVisitor<void, const JoinPath>
    {
    public:
        explicit FindDirectRightVisitor(CTEInfo & cte_info_, Poco::Logger * logger_) : cte_info(cte_info_), logger(logger_)
        {
        }

        void visitPlanNode(PlanNodeBase & node, const JoinPath &) override;
        void visitCTERefNode(CTERefNode & node, const JoinPath &) override;
        void visitJoinNode(JoinNode & node, const JoinPath &) override;

        CTEInfo & cte_info;
        Poco::Logger * logger;
        std::unordered_set<CTEId> deadlock_ctes;
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
        ExecuteOrderMap execute_orders;
        int cur_execute_order = 0;
    };

    class UpdateLeftExecuteOrderVisitor : public PlanNodeVisitor<void, const Void>
    {
    public:
        UpdateLeftExecuteOrderVisitor(CTEInfo & cte_info_, ExecuteOrderMap & execute_orders_, int execute_order_)
            : cte_info(cte_info_), execute_orders(execute_orders_), execute_order(execute_order_)
        {
        }

        void visitPlanNode(PlanNodeBase & node, const Void &) override;
        void visitCTERefNode(CTERefNode & node, const Void &) override;
        void visitJoinNode(JoinNode & node, const Void &) override;

        CTEInfo & cte_info;
        ExecuteOrderMap & execute_orders;
        const int execute_order = 0;
    };

    class AddBufferVisitor : public SimplePlanRewriter<const Void>
    {
    public:
        AddBufferVisitor(const std::unordered_set<CTEId> & deadlock_ctes_, ContextMutablePtr context_, CTEInfo & cte_info_)
            : SimplePlanRewriter<const Void>(std::move(context_), cte_info_), deadlock_ctes(deadlock_ctes_)
        {
        }

        PlanNodePtr visitCTERefNode(CTERefNode & node, const Void & c) override;

        const std::unordered_set<CTEId> & deadlock_ctes;
    };

    void FindDirectRightVisitor::visitPlanNode(PlanNodeBase & node, const JoinPath & join_path)
    {
        visit_path.emplace_back(&node, join_path);

        if (node.getChildren().empty() && join_path == JoinPath::RIGHT)
        {
            UpdateParentExecuteOrderVisitor update_parent_order_visitor{cte_info, visit_path};
            VisitorUtil::accept(node, update_parent_order_visitor, {});
            const auto & execute_orders = update_parent_order_visitor.execute_orders;

            if (logger && logger->is(Poco::Message::PRIO_TRACE))
            {
                std::ostringstream os;
                for (const auto & [node_id, node_orders] : execute_orders)
                    os << node_id << "->" << fmt::format("({})", fmt::join(node_orders, ",")) << " ";
                LOG_TRACE(logger, "Direct right node id: {}, calculated execute order: {}", node.getId(), os.str());
            }

            for (const auto & [cte_id, cte_def_node] : cte_info.getCTEs())
            {
                auto cte_def_node_id = cte_def_node->getId();
                if (execute_orders.count(cte_def_node_id) && execute_orders.at(cte_def_node_id).size() > 1)
                    deadlock_ctes.emplace(cte_id);
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
        execute_orders[node.getId()].emplace(cur_execute_order);

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
        execute_orders[node.getId()].emplace(++cur_execute_order);

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
        execute_orders[node.getId()].emplace(execute_order);

        for (auto & child : node.getChildren())
            VisitorUtil::accept(*child, *this, ctx);
    }

    void UpdateLeftExecuteOrderVisitor::visitCTERefNode(CTERefNode & node, const Void & ctx)
    {
        execute_orders[node.getId()].emplace(execute_order);

        auto cte_id = node.getStep()->getId();
        VisitorUtil::accept(*cte_info.getCTEDef(cte_id), *this, ctx);
    }

    void UpdateLeftExecuteOrderVisitor::visitJoinNode(JoinNode & node, const Void & ctx)
    {
        execute_orders[node.getId()].emplace(execute_order);

        VisitorUtil::accept(*node.getChildren().at(0), *this, ctx);
    }

    PlanNodePtr AddBufferVisitor::visitCTERefNode(CTERefNode & node, const Void & c)
    {
        SimplePlanRewriter<const Void>::visitCTERefNode(node, c);
        auto cte_id = node.getStep()->getId();

        if (!deadlock_ctes.count(cte_id))
        {
            return node.shared_from_this();
        }
        else
        {
            QueryPlanStepPtr buffer_step = std::make_shared<BufferStep>(node.getCurrentDataStream());
            PlanNodePtr buffer_node = PlanNodeBase::createPlanNode(
                context->nextNodeId(), std::move(buffer_step), {node.shared_from_this()}, node.getStatistics());
            return buffer_node;
        }
    }
}

void AddBufferForDeadlockCTE::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    static auto * logger = &Poco::Logger::get("AddBufferForDeadlockCTE");

    FindDirectRightVisitor find_deadlock_cte_visitor{plan.getCTEInfo(), logger};
    VisitorUtil::accept<void, const JoinPath>(plan.getPlanNode(), find_deadlock_cte_visitor, JoinPath::RIGHT);

    if (logger && logger->is(Poco::Message::PRIO_DEBUG))
    {
        std::ostringstream os;
        for (const auto & cte_id : find_deadlock_cte_visitor.deadlock_ctes)
            os << cte_id << '#' << plan.getCTEInfo().getCTEDef(cte_id)->getId() << ", ";
        LOG_DEBUG(logger, "Detected deadlock ctes(cte_id#plan_node_id): {}", os.str());
    }

    AddBufferVisitor add_buffer_visitor{find_deadlock_cte_visitor.deadlock_ctes, context, plan.getCTEInfo()};
    VisitorUtil::accept(plan.getPlanNode(), add_buffer_visitor, {});
}

}
