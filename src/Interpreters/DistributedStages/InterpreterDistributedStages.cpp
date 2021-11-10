#include <chrono>
#include <thread>
#include <Interpreters/DistributedStages/InterpreterDistributedStages.h>
#include <Interpreters/DistributedStages/InterpreterPlanSegment.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/IAST.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

InterpreterDistributedStages::InterpreterDistributedStages(const ASTPtr & query_ptr_, ContextPtr context_)
    : query_ptr(query_ptr_->clone())
    , context(Context::createCopy(context_))
    , log(&Poco::Logger::get("InterpreterDistributedStages"))
    , plan_segment_tree(std::make_unique<PlanSegmentTree>())
{
    initSettings();

    createPlanSegments();
}

void InterpreterDistributedStages::createPlanSegments()
{
    QueryPlan query_plan;
    InterpreterSelectWithUnionQuery(query_ptr, context, {}).buildQueryPlan(query_plan);
    PlanSegmentPtr plan_segment = std::make_unique<PlanSegment>();
    plan_segment->setQueryPlan(std::move(query_plan));

    PlanSegmentTree::Node node{.plan_segment = std::move(plan_segment)};

    plan_segment_tree->addNode(std::move(node));
}

BlockIO InterpreterDistributedStages::execute()
{
    return executePlanSegment();
}

BlockIO InterpreterDistributedStages::executePlanSegment()
{
    BlockIO res;

    PlanSegment * plan_segment = plan_segment_tree->getRoot()->getPlanSegment();
    res = InterpreterPlanSegment(plan_segment, context).execute();
    LOG_DEBUG(log, "Generate QueryPipeline from PlanSegment");

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
