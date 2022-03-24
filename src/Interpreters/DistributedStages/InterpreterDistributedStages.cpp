#include <chrono>
#include <memory>
#include <thread>
#include <Interpreters/DistributedStages/InterpreterDistributedStages.h>
#include <Interpreters/DistributedStages/InterpreterPlanSegment.h>
#include <Interpreters/DistributedStages/ExchangeStepVisitor.h>
#include <Interpreters/DistributedStages/PlanSegmentVisitor.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/RewriteDistributedQueryVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Client/Connection.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

InterpreterDistributedStages::InterpreterDistributedStages(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : query_ptr(query_ptr_->clone())
    , context(std::move(context_))
    , log(&Poco::Logger::get("InterpreterDistributedStages"))
    , plan_segment_tree(std::make_unique<PlanSegmentTree>())
{
    initSettings();

    createPlanSegments();
}

void InterpreterDistributedStages::createPlanSegments()
{
    /**
     * we collect all distributed tables and try to rewrite distributed query into local query because
     * distributed query cannot generate a plan with ScanTable so that it hard to build distributed plan segment on this
     * kind of plan.
     *
     * if there is any local table or there is no tables, we treat the query as original query so that it will
     * not be splited to several segments, instead it only has one segment and we will execute this segment in local server
     * as it original worked.
     */
    bool add_exchange = false;
    auto query_data = RewriteDistributedQueryMatcher::collectTableInfos(query_ptr, context);
    if (query_data.all_distributed && !query_data.table_rewrite_info.empty())
    {
        RewriteDistributedQueryVisitor(query_data).visit(query_ptr);
        add_exchange = true;
    }

    QueryPlan query_plan;
    SelectQueryOptions options;
    if (add_exchange)
        options.distributedStages();

    InterpreterSelectWithUnionQuery(query_ptr, context, options).buildQueryPlan(query_plan);

    if (add_exchange)
    {
        ExchangeStepContext exchange_context{.context = context, .query_plan = query_plan};
        AddExchangeRewriter::rewrite(query_plan, exchange_context);
    }

    WriteBufferFromOwnString plan_str;
    query_plan.explainPlan(plan_str, {});
    LOG_DEBUG(log, "QUERY-PLAN-AFTER-EXCHANGE \n" + plan_str.str());

    PlanSegmentContext plan_segment_context{.context = context,
                                            .query_plan = query_plan,
                                            .query_id = context->getCurrentQueryId(),
                                            .shard_number = query_data.cluster ? query_data.cluster->getShardCount() : 1,
                                            .cluster_name = query_data.cluster_name,
                                            .plan_segment_tree = plan_segment_tree.get()};
    PlanSegmentSpliter::rewrite(query_plan, plan_segment_context);

    LOG_DEBUG(log, "PLAN-SEGMENTS \n" + plan_segment_tree->toString());
}

BlockIO InterpreterDistributedStages::execute()
{
    return executePlanSegment();
}

PlanSegmentPtr MockPlanSegment(ContextPtr context)
{
    PlanSegmentPtr plan_segment = std::make_unique<PlanSegment>();

    PlanSegmentInputPtr left = std::make_shared<PlanSegmentInput>(PlanSegmentType::EXCHANGE);
    PlanSegmentInputPtr right = std::make_shared<PlanSegmentInput>(PlanSegmentType::EXCHANGE);
    PlanSegmentOutputPtr output = std::make_shared<PlanSegmentOutput>(PlanSegmentType::OUTPUT);

    plan_segment->appendPlanSegmentInput(left);
    plan_segment->appendPlanSegmentInput(right);
    plan_segment->setPlanSegmentOutput(output);

    /***
     * only read from system.one
     */
    StorageID table_id = StorageID("system", "one");
    StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, context);


    QueryPlan query_plan;
    SelectQueryInfo select_query_info;
    storage->read(query_plan, {"dummy"}, storage->getInMemoryMetadataPtr(), select_query_info, context, {}, 0, 0);

    plan_segment->setQueryPlan(std::move(query_plan));

    return plan_segment;
}

void MockSendPlanSegment(ContextPtr query_context)
{
    auto plan_segment = MockPlanSegment(query_context);

    auto cluster = query_context->getCluster("test_shard_localhost");

    /**
     * only get the current node
     */
    auto node = cluster->getShardsAddresses().back()[0];

    auto connection = std::make_shared<Connection>(
                    node.host_name, node.port, node.default_database,
                    node.user, node.password, node.cluster, node.cluster_secret,
                    "MockSendPlanSegment", node.compression, node.secure);

    const auto & settings = query_context->getSettingsRef();
    auto connection_timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    connection->sendPlanSegment(connection_timeouts, plan_segment.get(), &settings, &query_context->getClientInfo());
    connection->poll(1000);
    Packet packet = connection->receivePacket();
    LOG_TRACE(&Poco::Logger::get("MockSendPlanSegment"), "sendPlanSegmentToLocal finish:" + std::to_string(packet.type));
    switch (packet.type)
    {
        case Protocol::Server::Exception:
            throw *packet.exception;
        case Protocol::Server::EndOfStream:
            break;
        default:
            throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    }
    connection->disconnect();
}

void checkPlan(PlanSegment * lhs, PlanSegment * rhs)
{
    auto lhs_str = lhs->toString();
    auto rhs_str = rhs->toString();

    if(lhs_str != rhs_str)
        throw Exception("checkPlan failed", ErrorCodes::LOGICAL_ERROR);
}

void MockTestQuery(PlanSegmentTree * plan_segment_tree, ContextMutablePtr context)
{
    if (plan_segment_tree->getNodes().size() < 2)
        return;
    /**
     * serialize to buffer
     */
    WriteBufferFromOwnString write_buffer;
    writeBinary(plan_segment_tree->getNodes().size(), write_buffer);
    for (auto & node : plan_segment_tree->getNodes())
        node.plan_segment->serialize(write_buffer);

    ReadBufferFromString read_buffer(write_buffer.str());
    size_t plan_size;
    readBinary(plan_size, read_buffer);
    std::vector<std::shared_ptr<PlanSegment>> plansegments;

    for (size_t i = 0; i < plan_size; ++i)
    {
        auto plan = std::make_shared<PlanSegment>(context);
        plan->deserialize(read_buffer);
        plansegments.push_back(plan);
    }

    /**
     * check results
     */
    std::vector<PlanSegment *> old_plans;
    for (auto & node : plan_segment_tree->getNodes())
        old_plans.push_back(node.plan_segment.get());
    
    for (size_t i = 0; i < plan_size; ++i)
    {
        auto lhs = old_plans[i];
        auto rhs = plansegments[i].get();
        checkPlan(lhs, rhs);
    }
}

BlockIO InterpreterDistributedStages::executePlanSegment()
{
    BlockIO res;

    LOG_DEBUG(log, "Generate QueryPipeline from PlanSegment");

    PlanSegmentsStatusPtr scheduler_status;
    
    if (plan_segment_tree->getNodes().size() > 1)
        scheduler_status = context->getSegmentScheduler()->insertPlanSegments(context->getClientInfo().initial_query_id, plan_segment_tree.get(), context);
    else
    {
        scheduler_status = std::make_shared<PlanSegmentsStatus>();
        scheduler_status->is_final_stage_start = true;
    }

    if (!scheduler_status)
        throw Exception("Cannot get scheduler status from segment scheduler", ErrorCodes::LOGICAL_ERROR);

    Stopwatch s;
    while (1)
    {
        if (context->getSettingsRef().max_execution_time.value.seconds() > 0 && s.elapsedSeconds() > context->getSettingsRef().max_execution_time.value.seconds())
            throw Exception("Final stage not start", ErrorCodes::LOGICAL_ERROR);

        if (scheduler_status->is_final_stage_start)
        {
            auto * final_segment = plan_segment_tree->getRoot()->getPlanSegment();
            final_segment->update();
            LOG_TRACE(log, "EXECUTE\n" + final_segment->toString());

            if (context->getSettingsRef().debug_plan_generation)
                break;            
            res = DB::lazyExecutePlanSegmentLocally(std::make_unique<PlanSegment>(std::move(*final_segment)), context);
            break;
        }
    }

    return res;
}

void InterpreterDistributedStages::initSettings()
{
    auto query = getQuery();
    const auto * select_with_union = query->as<ASTSelectWithUnionQuery>();
    const auto * select_query = query->as<ASTSelectQuery>();

    if (!select_with_union && !select_query)
        return;

    if (select_with_union)
    {
        size_t num_selects = select_with_union->list_of_selects->children.size();
        if (num_selects == 1)
            select_query = typeid_cast<ASTSelectQuery *>(select_with_union->list_of_selects->children.at(0).get());
    }

    if (select_query && select_query->settings())
        InterpreterSetQuery(select_query->settings(), context).executeForCurrentContext();
}

bool InterpreterDistributedStages::isDistributedStages(const ASTPtr & query, ContextPtr context_)
{
    const auto * select_with_union = query->as<ASTSelectWithUnionQuery>();
    const auto * select_query = query->as<ASTSelectQuery>();

    if (!select_with_union && !select_query)
        return false;

    if (select_with_union)
    {
        size_t num_selects = select_with_union->list_of_selects->children.size();
        if (num_selects == 1)
            select_query = typeid_cast<ASTSelectQuery *>(select_with_union->list_of_selects->children.at(0).get());
    }

    if (select_query && select_query->settings())
    {
        ContextMutablePtr context_clone = Context::createCopy(context_);
        InterpreterSetQuery(select_query->settings(), context_clone).executeForCurrentContext();
        return context_clone->getSettingsRef().enable_distributed_stages;
    }

    return context_->getSettingsRef().enable_distributed_stages;
}

DistributedStagesSettings InterpreterDistributedStages::extractDistributedStagesSettingsImpl(const ASTPtr & query, ContextPtr context_)
{
    const auto * select_with_union = query->as<ASTSelectWithUnionQuery>();
    const auto * select_query = query->as<ASTSelectQuery>();

    if (!select_with_union && !select_query)
        return DistributedStagesSettings(false, false);

    const Settings & settings_ = context_->getSettingsRef();
    DistributedStagesSettings distributed_stages_settings(settings_.enable_distributed_stages, settings_.fallback_to_simple_query);

    if (select_with_union)
    {
        size_t num_selects = select_with_union->list_of_selects->children.size();
        if (num_selects == 1)
            select_query = typeid_cast<ASTSelectQuery *>(select_with_union->list_of_selects->children.at(0).get());
    }

    if (select_query && select_query->settings())
    {
        ContextMutablePtr context_clone = Context::createCopy(context_);
        InterpreterSetQuery(select_query->settings(), context_clone).executeForCurrentContext();
        const Settings & clone_settings = context_clone->getSettingsRef();
        distributed_stages_settings.enable_distributed_stages = clone_settings.enable_distributed_stages;
        distributed_stages_settings.fallback_to_simple_query = clone_settings.fallback_to_simple_query;
        return distributed_stages_settings;
    }

    return distributed_stages_settings;
}

DistributedStagesSettings InterpreterDistributedStages::extractDistributedStagesSettings(const ASTPtr & query, ContextPtr context_)
{
    const auto * insert_query = query->as<ASTInsertQuery>();
    if (insert_query && insert_query->select)
        return extractDistributedStagesSettingsImpl(insert_query->select, context_);
    else
        return extractDistributedStagesSettingsImpl(query, context_);
}

}
