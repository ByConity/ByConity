#include <Interpreters/DistributedStages/ExchangeStepVisitor.h>
#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/ExchangeStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/QueryPlan/JoinStep.h>

namespace DB
{

ExchangeStepResult ExchangeStepVisitor::visitPlan(QueryPlan::Node * node, ExchangeStepContext & exchange_context)
{
    for (size_t i = 0; i < node->children.size(); ++i)
    {
        auto result_node = visitChild(node->children[i], exchange_context);
        if (result_node)
            node->children[i] = result_node;
    }

    return nullptr;
}

ExchangeStepResult ExchangeStepVisitor::visitChild(QueryPlan::Node * node, ExchangeStepContext & exchange_context)
{
    return VisitorUtil::accept(node, *this, exchange_context);
}

ExchangeStepResult ExchangeStepVisitor::visitMergingAggregatedStep(QueryPlan::Node * node, ExchangeStepContext & exchange_context)
{
    visitPlan(node, exchange_context);

    MergingAggregatedStep * step = dynamic_cast<MergingAggregatedStep *>(node->step.get());

    auto params = step->getParams()->params;

    std::cout<<" header from params: " << params.getHeader(false) << std::endl;
    std::cout<<" header from aggregator: " << step->getParams()->getHeader() << std::endl;

    Block result_header = params.getHeader(false);

    Names keys;
    for (auto & index : params.keys)
        keys.push_back(result_header.safeGetByPosition(index).name);

    auto exchange_step = std::make_unique<ExchangeStep>(step->getInputStreams(), ExchangeMode::REPARTITION, Partitioning(keys));
    QueryPlan::Node exchange_node{.step = std::move(exchange_step)};
    exchange_context.query_plan.addNode(std::move(exchange_node));
    auto last_node = exchange_context.query_plan.getLastNode();
    last_node->children.push_back(last_node);
    node->children.swap(last_node->children);

    return nullptr;
}

ExchangeStepResult ExchangeStepVisitor::visitJoinStep(QueryPlan::Node * node, ExchangeStepContext & exchange_context)
{
    visitPlan(node, exchange_context);

    JoinStep * step = dynamic_cast<JoinStep *>(node->step.get());
    auto join = step->getJoin();
    auto join_infos = join->getTableJoin();

    auto left_keys = join_infos.keyNamesLeft();
    auto right_keys = join_infos.keyNamesRight();

    auto locality = join_infos.locality();

    if (node->children.size() != 2)
        throw Exception("Join must have two children", ErrorCodes::LOGICAL_ERROR);

    /**
     * Left join is always index 0 when query plan is generated 
     */
    auto left_stream = step->getInputStreams()[0];
    auto right_stream = step->getInputStreams()[1];

    auto add_exchange = [&](DataStream data_stream, size_t index, ExchangeMode mode, const Names & keys)
    {
        auto exchange_step = std::make_unique<ExchangeStep>(DataStreams{data_stream}, mode, Partitioning(keys));
        QueryPlan::Node exchange_node{.step = std::move(exchange_step), .children = {node->children[index]}};

        exchange_context.query_plan.addNode(std::move(exchange_node));
        node->children[index] = exchange_context.query_plan.getLastNode();
    };

    if (locality == ASTTableJoin::Locality::Local)
    {
        add_exchange(left_stream, 0, ExchangeMode::LOCAL_NO_NEED_REPARTITION, Names{});
        add_exchange(right_stream, 1, ExchangeMode::LOCAL_NO_NEED_REPARTITION, Names{});
    }
    else if (locality == ASTTableJoin::Locality::Global)
    {
        add_exchange(left_stream, 0, ExchangeMode::LOCAL_NO_NEED_REPARTITION, Names{});
        add_exchange(right_stream, 1, ExchangeMode::BROADCAST, Names{});
    }
    else
    {
        add_exchange(left_stream, 0, ExchangeMode::REPARTITION, left_keys);
        add_exchange(right_stream, 1, ExchangeMode::REPARTITION, right_keys);
    }

    return nullptr;
}

void AddExchangeRewriter::rewrite(QueryPlan & query_plan, ExchangeStepContext & exchange_context)
{
    ExchangeStepVisitor visitor{};
    ExchangeStepResult result_node = VisitorUtil::accept(query_plan.getRoot(), visitor, exchange_context);
    if (result_node)
        query_plan.setRoot(result_node);
}


}
