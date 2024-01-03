/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/QueryRewriter.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/MPPQueryCoordinator.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Optimizer/JoinOrderUtils.h>
#include <Optimizer/PlanNodeSearcher.h>
#include <Optimizer/PlanOptimizer.h>
#include <Parsers/ASTTEALimit.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/PlanPrinter.h>
#include <QueryPlan/PlanCache.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/QueryPlanner.h>
#include <QueryPlan/PlanPrinter.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageDistributed.h>
#include <Interpreters/WorkerStatusManager.h>
#include <common/logger_useful.h>
#include <Storages/Hive/StorageCnchHive.h>
#include <Storages/RemoteFile/IStorageCnchFile.h>
#include "QueryPlan/QueryPlan.h"
#include <Parsers/queryToString.h>
#include <Interpreters/executeQuery.h>
#include <common/logger_useful.h>
#include <QueryPlan/PlanNodeIdAllocator.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_MANY_PLAN_SEGMENTS;
}

Block InterpreterSelectQueryUseOptimizer::getSampleBlock()
{
    if (!block)
    {
        auto query_plan = buildQueryPlan();
    }

    return block;
}

QueryPlanPtr InterpreterSelectQueryUseOptimizer::buildQueryPlan()
{
    // When interpret sub query, reuse context info, e.g. PlanNodeIdAllocator, SymbolAllocator.
    if (interpret_sub_query)
    {
        QueryPlanPtr sub_query_plan = std::make_unique<QueryPlan>(sub_plan_ptr, cte_info, context->getPlanNodeIdAllocator());
        PlanOptimizer::optimize(*sub_query_plan, context);
        return sub_query_plan;
    }

    QueryPlanPtr query_plan;
    UInt128 query_hash;
    context->createPlanNodeIdAllocator();
    context->createSymbolAllocator();
    context->createOptimizerMetrics();
    // not cache internal query
    bool enable_plan_cache = !options.is_internal && PlanCacheManager::enableCachePlan(query_ptr, context);

    {
        if (enable_plan_cache)
        {
            query_hash = PlanCacheManager::hash(query_ptr, context->getSettingsRef());
            query_plan = PlanCacheManager::getPlanFromCache(query_hash, context);
            if (query_plan)
            {
                query_plan->addInterpreterContext(context);
                LOG_INFO(log, "hit plan cache");
            }
        }

        if (!query_plan)
        {
            Stopwatch stage_watch;
            stage_watch.start();
            auto cloned_query = query_ptr->clone();
            cloned_query = QueryRewriter().rewrite(cloned_query, context);
            context->logOptimizerProfile(
                log, "Optimizer stage run time: ", "Rewrite", std::to_string(stage_watch.elapsedMillisecondsAsDouble()) + "ms");

            stage_watch.restart();
            AnalysisPtr analysis = QueryAnalyzer::analyze(cloned_query, context);
            fillContextQueryAccessInfo(context, analysis);
            context->logOptimizerProfile(
                log, "Optimizer stage run time: ", "Analyzer", std::to_string(stage_watch.elapsedMillisecondsAsDouble()) + "ms");
            stage_watch.restart();
            query_plan = QueryPlanner().plan(cloned_query, *analysis, context);
            context->logOptimizerProfile(
                log, "Optimizer stage run time: ", "Planning", std::to_string(stage_watch.elapsedMillisecondsAsDouble()) + "ms");

            stage_watch.restart();
            PlanOptimizer::optimize(*query_plan, context);
            context->logOptimizerProfile(
                log, "Optimizer stage run time: ", "Optimizer", std::to_string(stage_watch.elapsedMillisecondsAsDouble()) + "ms");
            if (enable_plan_cache && query_hash && query_plan)
            {
               if (PlanCacheManager::addPlanToCache(query_hash, query_plan, analysis, context))
                   LOG_INFO(log, "plan cache added");
            }
        }
    }

    if (query_plan->getPlanNodeRoot())
        block = query_plan->getPlanNodeRoot()->getCurrentDataStream().header;
    LOG_DEBUG(log, "join order {}", JoinOrderUtils::getJoinOrder(*query_plan));
    return query_plan;
}

std::pair<PlanSegmentTreePtr, std::set<StorageID>> InterpreterSelectQueryUseOptimizer::getPlanSegment()
{
    Stopwatch stage_watch, total_watch;
    total_watch.start();
    setUnsupportedSettings(context);
    QueryPlanPtr query_plan = buildQueryPlan();

    query_plan->setResetStepId(false);
    stage_watch.start();
    QueryPlan plan = PlanNodeToNodeVisitor::convert(*query_plan);

    LOG_DEBUG(log, "optimizer stage run time: plan normalize, {} ms", stage_watch.elapsedMillisecondsAsDouble());
    stage_watch.restart();

    PlanSegmentTreePtr plan_segment_tree = std::make_unique<PlanSegmentTree>();
    ClusterInfoContext cluster_info_context{.query_plan = *query_plan, .context = context, .plan_segment_tree = plan_segment_tree};
    PlanSegmentContext plan_segment_context = ClusterInfoFinder::find(*query_plan, cluster_info_context);

    stage_watch.restart();
    std::set<StorageID> used_storage_ids = plan.allocateLocalTable(context);
    // select health worker before split
    if (context->getSettingsRef().enable_adaptive_scheduler && context->tryGetCurrentWorkerGroup())
    {
        context->selectWorkerNodesWithMetrics();
        auto wg_health = context->getWorkerGroupStatusPtr()->getWorkerGroupHealth();
        if (wg_health == WorkerGroupHealthStatus::Critical)
            throw Exception("no worker available", ErrorCodes::LOGICAL_ERROR);
    }

    PlanSegmentSplitter::split(plan, plan_segment_context);
    context->logOptimizerProfile(
        log, "Optimizer total run time: ", "PlanSegment build", std::to_string(stage_watch.elapsedMillisecondsAsDouble()) + "ms");

    setPlanSegmentInfoForExplainAnalyze(plan_segment_tree);
    GraphvizPrinter::printPlanSegment(plan_segment_tree, context);
    context->logOptimizerProfile(
        log, "Optimizer total run time: ", "Optimizer Total", std::to_string(total_watch.elapsedMillisecondsAsDouble()) + "ms");

    if (context->getSettingsRef().log_segment_profiles)
    {
        segment_profiles = std::make_shared<std::vector<String>>();
        for (auto & node : plan_segment_tree->getNodes())
            segment_profiles->emplace_back(PlanSegmentDescription::getPlanSegmentDescription(node.plan_segment, true)->jsonPlanSegmentDescriptionAsString({}));
    }

    return std::make_pair(std::move(plan_segment_tree), std::move(used_storage_ids));
}

QueryPipeline executeTEALimit(QueryPipeline & pipeline, ContextMutablePtr context, ASTPtr query_ptr)
{
    const ASTSelectWithUnionQuery & ast = query_ptr->as<ASTSelectWithUnionQuery &>();

    // Create implicit storage to buffer pre tealimit results
    NamesAndTypesList columns = pipeline.getHeader().getNamesAndTypesList();
    auto temporary_table = TemporaryTableHolder(context->getQueryContext(), ColumnsDescription{columns}, {}, {}, {});

    String implicit_name = "_TEALIMITDATA";

    auto storage = temporary_table.getTable();
    BlockOutputStreamPtr output = storage->write(ASTPtr(), storage->getInMemoryMetadataPtr(), context);

    PullingAsyncPipelineExecutor executor(pipeline);
    Block block;

    output->writePrefix();
    while (executor.pull(block, context->getSettingsRef().interactive_delay / 1000))
    {
        if (block)
            output->write(block);
    }
    output->writeSuffix();

    // Query level temporary table
    context->getQueryContext()->addExternalTable(implicit_name, std::move(temporary_table));

    // Construct the internal SQL
    //
    // select t,  g_0, g_1, ...., g_n, cnt_0 ,..., cnt_n from misc_online_all WHERE xxx group by t, g0, g_1...gn
    // TEALIMIT N /*METRIC cnt_0, ..., cnt_n*/ GROUP (g_0, ... , g_n) ORDER EXPR(cnt_0, ... cnt_n) ASC|DESC
    //
    // select t, g_0, g_1, ..., cnt_0, ..., cnt_n from implicit_storage where
    // （g_0, ..., g_n) in (select g_0, ...., g_n from implicit_storage
    //  group by g_0, ..., g_n order by EXPR(sum(cnt_0), ..., sum(cnt_n)) ASC|DESC LIMIT N)
    //
    //
    std::stringstream postQuery;
    postQuery << "SELECT ";

    auto implicit_select_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & column : columns)
    {
        implicit_select_expr_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
    }
    postQuery << queryToString(*implicit_select_expr_list) << " FROM  " << implicit_name << " WHERE ";

    bool tealimit_order_keep = context->getSettingsRef().tealimit_order_keep;

    auto tea_limit = dynamic_cast<ASTTEALimit *>(ast.tealimit.get());
    String g_list_str = queryToString(*tea_limit->group_expr_list);
    postQuery << "(" << g_list_str << ") IN (";
    // SUBQUERY
    postQuery << "SELECT " << g_list_str << " FROM  " << implicit_name << " GROUP BY " << g_list_str;

    postQuery << " ORDER BY ";
    auto o_list = tea_limit->order_expr_list->clone(); // will rewrite
    ASTs & elems = o_list->children;

    // Check Whether order list is in group by list, if yes, we don't add implicit
    // SUM clause
    auto nodeInGroup = [&](ASTPtr group_expr_list, const ASTPtr & node) -> bool {
        if (!tealimit_order_keep)
            return false;

        // special handling group (g0, g1), case where group expr is tuple function, step forward
        // to get g0, g1
        if (group_expr_list->children.size() == 1)
        {
            const auto * tupleFunc = group_expr_list->children[0]->as<ASTFunction>();
            if (tupleFunc && tupleFunc->name == "tuple")
            {
                group_expr_list = group_expr_list->children[0]->children[0];
            }
        }

        for (auto & g : group_expr_list->children)
        {
            if (node->getAliasOrColumnName() == g->getAliasOrColumnName())
            {
                return true;
            }
        }
        return false;
    };

    bool comma = false;
    for (auto & elem : elems)
    {
        auto orderCol = elem->children.front();

        if (!nodeInGroup(tea_limit->group_expr_list, orderCol))
        {
            // check if orderCol is ASTFunction or ASTIdentifier, rewrite it
            const ASTFunction * func = orderCol->as<ASTFunction>();
            const ASTIdentifier * identifier = orderCol->as<ASTIdentifier>();
            if (identifier)
            {
                auto sum_function = std::make_shared<ASTFunction>();
                sum_function->name = "SUM";
                sum_function->arguments = std::make_shared<ASTExpressionList>();
                sum_function->children.push_back(sum_function->arguments);
                sum_function->arguments->children.push_back(orderCol);

                // ORDER BY SUM()
                elem->children[0] = std::move(sum_function);
            }
            else if (func)
            {
                size_t numArgs = func->arguments->children.size();
                for (size_t i = 0; i < numArgs; i++)
                {
                    auto & iArg = func->arguments->children[i];
                    if (nodeInGroup(tea_limit->group_expr_list, iArg))
                        continue;
                    auto sum_function = std::make_shared<ASTFunction>();
                    sum_function->name = "SUM";
                    sum_function->arguments = std::make_shared<ASTExpressionList>();
                    sum_function->children.push_back(sum_function->arguments);
                    sum_function->arguments->children.push_back(iArg);

                    // inplace replace EXPR with new argument
                    func->arguments->children[i] = std::move(sum_function);
                }
            }
            else
            {
                throw Exception("TEALimit unhandled " + queryToString(*elem), ErrorCodes::LOGICAL_ERROR);
            }
        }

        const ASTOrderByElement & order_by_elem = elem->as<ASTOrderByElement &>();

        if (comma)
            postQuery << ", ";
        comma = true; // skip the first one
        postQuery << serializeAST(order_by_elem, true);
    }

    postQuery << " LIMIT ";
    if (tea_limit->limit_offset)
    {
        postQuery << serializeAST(*tea_limit->limit_offset, true) << ", ";
    }
    postQuery << serializeAST(*tea_limit->limit_value, true);
    postQuery << ")";

    //@user-profile, TEALIMIT output need respect order by info
    if (tealimit_order_keep)
    {
        comma = false;
        postQuery << " ORDER BY ";
        for (auto & elem : tea_limit->order_expr_list->children)
        {
            if (comma)
                postQuery << ", ";
            comma = true;
            postQuery << serializeAST(*elem, true);
        }
    }

    // evaluate the internal SQL and get the result
    return executeQuery(postQuery.str(), context->getQueryContext(), true).pipeline;
}

BlockIO InterpreterSelectQueryUseOptimizer::execute()
{
    std::pair<PlanSegmentTreePtr, std::set<StorageID>> plan_segment_tree_and_used_storage_ids = getPlanSegment();
    auto & plan_segment_tree = plan_segment_tree_and_used_storage_ids.first;
    size_t plan_segment_num = plan_segment_tree->getNodes().size();
    UInt64 max_plan_segment_num = context->getSettingsRef().max_plan_segment_num;
    if (max_plan_segment_num != 0 && plan_segment_num > max_plan_segment_num)
        throw Exception(
            fmt::format(
                "query_id:{} plan_segments size {} exceed max_plan_segment_num {}",
                context->getCurrentQueryId(),
                plan_segment_num,
                max_plan_segment_num),
            ErrorCodes::TOO_MANY_PLAN_SEGMENTS);

    auto coodinator = std::make_shared<MPPQueryCoordinator>(std::move(plan_segment_tree), context, MPPQueryOptions());

    BlockIO res = coodinator->execute();

    if (auto * select_union = query_ptr->as<ASTSelectWithUnionQuery>())
    {
        if (unlikely(select_union->tealimit))
            res.pipeline = executeTEALimit(res.pipeline, context, query_ptr);
    }

    res.pipeline.addUsedStorageIDs(plan_segment_tree_and_used_storage_ids.second);
    return res;
}

void InterpreterSelectQueryUseOptimizer::setPlanSegmentInfoForExplainAnalyze(PlanSegmentTreePtr & plan_segment_tree)
{
    auto * final_segment = plan_segment_tree->getRoot()->getPlanSegment();
    if (final_segment->getQueryPlan().getRoot())
    {
        ExplainAnalyzeVisitor explain_visitor;
        VisitorUtil::accept(final_segment->getQueryPlan().getRoot(), explain_visitor, plan_segment_tree->getNodes());
    }
}

void InterpreterSelectQueryUseOptimizer::fillContextQueryAccessInfo(ContextPtr context, AnalysisPtr & analysis)
{
    if (context->hasQueryContext())
    {
        const auto & used_columns_map = analysis->getUsedColumns();
        for (const auto & [table_ast, storage_analysis] : analysis->getStorages())
        {
            Names required_columns;
            auto storage_id = storage_analysis.storage->getStorageID();
            if (auto it = used_columns_map.find(storage_analysis.storage->getStorageID()); it != used_columns_map.end())
            {
                for (const auto & column : it->second)
                    required_columns.emplace_back(column);
            }
            context->getQueryContext()->addQueryAccessInfo(
                backQuoteIfNeed(storage_id.getDatabaseName()),
                storage_id.getFullTableName(),
                required_columns);
        }
    }
}

void InterpreterSelectQueryUseOptimizer::setUnsupportedSettings(ContextMutablePtr & context)
{
    if (!context->getSettingsRef().enable_optimizer)
        return;

    SettingsChanges setting_changes;
    setting_changes.emplace_back("distributed_aggregation_memory_efficient", false);

    context->applySettingsChanges(setting_changes);
}

QueryPlan PlanNodeToNodeVisitor::convert(QueryPlan & query_plan)
{
    QueryPlan plan;
    PlanNodeToNodeVisitor visitor(plan);
    Void c;
    auto * root = VisitorUtil::accept(query_plan.getPlanNode(), visitor, c);
    plan.setRoot(root);

    for (const auto & cte : query_plan.getCTEInfo().getCTEs())
        plan.getCTENodes().emplace(cte.first, VisitorUtil::accept(cte.second, visitor, c));
    return plan;
}

QueryPlan::Node * PlanNodeToNodeVisitor::visitPlanNode(PlanNodeBase & node, Void & c)
{
    if (node.getChildren().empty())
    {
        auto res = QueryPlan::Node{.step = std::const_pointer_cast<IQueryPlanStep>(node.getStep()), .children = {}, .id = node.getId()};
        node.setStep(res.step);
        plan.addNode(std::move(res));
        return plan.getLastNode();
    }

    std::vector<QueryPlan::Node *> children;
    for (const auto & item : node.getChildren())
    {
        auto * child = VisitorUtil::accept(*item, *this, c);
        children.emplace_back(child);
    }
    QueryPlan::Node query_plan_node{
        .step = std::const_pointer_cast<IQueryPlanStep>(node.getStep()), .children = children, .id = node.getId()};
    node.setStep(query_plan_node.step);
    plan.addNode(std::move(query_plan_node));
    return plan.getLastNode();
}

PlanSegmentContext ClusterInfoFinder::find(QueryPlan & plan, ClusterInfoContext & cluster_info_context)
{
    ClusterInfoFinder visitor{plan.getCTEInfo()};

    // default schedule to worker cluster
    std::optional<PlanSegmentContext> result = VisitorUtil::accept(plan.getPlanNode(), visitor, cluster_info_context);
    if (result.has_value())
    {
        return result.value();
    }

    // if query is a constant query, like, select 1, schedule to server (coordinator)
    PlanSegmentContext plan_segment_context{
        .context = cluster_info_context.context,
        .query_plan = cluster_info_context.query_plan,
        .query_id = cluster_info_context.context->getCurrentQueryId(),
        .shard_number = 1,
        .cluster_name = "",
        .plan_segment_tree = cluster_info_context.plan_segment_tree.get()};
    return plan_segment_context;
}

std::optional<PlanSegmentContext> ClusterInfoFinder::visitPlanNode(PlanNodeBase & node, ClusterInfoContext & cluster_info_context)
{
    for (const auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(child, *this, cluster_info_context);
        if (result.has_value())
            return result;
    }
    return std::nullopt;
}

std::optional<PlanSegmentContext> ClusterInfoFinder::visitTableScanNode(TableScanNode & node, ClusterInfoContext & cluster_info_context)
{
    auto source_step = node.getStep();
    const auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(source_step->getStorage().get());
    const auto * cnch_hive = dynamic_cast<StorageCnchHive *>(source_step->getStorage().get());
    const auto * cnch_file = dynamic_cast<IStorageCnchFile *>(source_step->getStorage().get());

    if (cnch_table || cnch_hive || cnch_file)
    {
        const auto & worker_group = cluster_info_context.context->getCurrentWorkerGroup();
        auto worker_group_status_ptr = cluster_info_context.context->getWorkerGroupStatusPtr();
        PlanSegmentContext plan_segment_context{
            .context = cluster_info_context.context,
            .query_plan = cluster_info_context.query_plan,
            .query_id = cluster_info_context.context->getCurrentQueryId(),
            .shard_number = worker_group->getShardsInfo().size(),
            .cluster_name = worker_group->getID(),
            .plan_segment_tree = cluster_info_context.plan_segment_tree.get(),
            .health_parallel
            = worker_group_status_ptr ? std::optional<size_t>(worker_group_status_ptr->getAvaiableComputeWorkerSize()) : std::nullopt};
        return plan_segment_context;
    }
    return std::nullopt;
}

std::optional<PlanSegmentContext> ClusterInfoFinder::visitTableWriteNode(TableWriteNode & node, ClusterInfoContext & cluster_info_context)
{
    auto child_res = ClusterInfoFinder::visitPlanNode(node, cluster_info_context);
    if (child_res.has_value())
    {
        return child_res;
    }

    auto source_step = node.getStep();
    auto storage = source_step->getTarget()->getStorage();
    const auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(storage.get());
    const auto * cnch_hive = dynamic_cast<StorageCnchHive *>(storage.get());
    const auto * cnch_file = dynamic_cast<IStorageCnchFile *>(storage.get());

    if (cnch_table || cnch_hive || cnch_file)
    {
        const auto & worker_group = cluster_info_context.context->getCurrentWorkerGroup();
        auto worker_group_status_ptr = cluster_info_context.context->getWorkerGroupStatusPtr();
        PlanSegmentContext plan_segment_context{
            .context = cluster_info_context.context,
            .query_plan = cluster_info_context.query_plan,
            .query_id = cluster_info_context.context->getCurrentQueryId(),
            .shard_number =  worker_group->getShardsInfo().size(),
            .cluster_name = worker_group->getID(),
            .plan_segment_tree = cluster_info_context.plan_segment_tree.get(),
            .health_parallel = worker_group_status_ptr ?
                std::optional<size_t>(worker_group_status_ptr->getAvaiableComputeWorkerSize()) : std::nullopt};

        return plan_segment_context;
    }
    return std::nullopt;
}

std::optional<PlanSegmentContext> ClusterInfoFinder::visitCTERefNode(CTERefNode & node, ClusterInfoContext & cluster_info_context)
{
    const auto * cte = dynamic_cast<const CTERefStep *>(node.getStep().get());
    return cte_helper.accept(cte->getId(), *this, cluster_info_context);
}

void ExplainAnalyzeVisitor::visitExplainAnalyzeNode(QueryPlan::Node * node, PlanSegmentTree::Nodes & nodes)
{
    auto * explain = dynamic_cast<ExplainAnalyzeStep *>(node->step.get());
    if (explain->getKind() != ASTExplainQuery::ExplainKind::DistributedAnalyze && explain->getKind() != ASTExplainQuery::ExplainKind::PipelineAnalyze)
        return;
    PlanSegmentDescriptions plan_segment_descriptions;
    bool record_plan_detail = explain->getSetting().json && (explain->getKind() != ASTExplainQuery::ExplainKind::PipelineAnalyze);
    for (auto & segment_node : nodes)
        plan_segment_descriptions.emplace_back(PlanSegmentDescription::getPlanSegmentDescription(segment_node.plan_segment, record_plan_detail));
    explain->setPlanSegmentDescriptions(plan_segment_descriptions);
}

void ExplainAnalyzeVisitor::visitNode(QueryPlan::Node * node, PlanSegmentTree::Nodes & nodes)
{
    for (const auto & child : node->children)
        VisitorUtil::accept(child, *this, nodes);
}

}
