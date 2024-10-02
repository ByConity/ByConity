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
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>

#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/QueryRewriter.h>
#include <Analyzers/SubstituteLiteralToPreparedParams.h>
#include <DataTypes/ObjectUtils.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Cache/QueryCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/MPPQueryCoordinator.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/PreparedStatement/PreparedStatementManager.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/executeQuery.h>
#include <Optimizer/JoinOrderUtils.h>
#include <Optimizer/PlanNodeSearcher.h>
#include <Optimizer/PlanOptimizer.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTPreparedStatement.h>
#include <Parsers/ASTTEALimit.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <QueryPlan/FinalSampleStep.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/PlanCache.h>
#include <QueryPlan/PlanNodeIdAllocator.h>
#include <QueryPlan/PlanPrinter.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/QueryPlanner.h>
#include <Storages/Hive/StorageCnchHive.h>
#include <Storages/RemoteFile/IStorageCnchFile.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageDistributed.h>
#include <google/protobuf/util/json_util.h>
#include <Poco/Logger.h>
#include "common/defines.h"
#include <Common/ProfileEvents.h>
#include <common/logger_useful.h>
#include "Interpreters/ClientInfo.h"
#include "Interpreters/Context_fwd.h"


namespace ProfileEvents
{
    extern const Event QueryRewriterTime;
    extern const Event QueryAnalyzerTime;
    extern const Event QueryPlannerTime;
    extern const Event QueryOptimizerTime;
    extern const Event PlanSegmentSplitterTime;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_MANY_PLAN_SEGMENTS;
    extern const int OPTIMIZER_NONSUPPORT;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_PLAN_SEGMENT;
    extern const int PLAN_CACHE_NOT_USED;
    extern const int BAD_PREPARED_PARAMETER;
}

Block InterpreterSelectQueryUseOptimizer::getSampleBlock()
{
    if (!block)
    {
        auto query_plan = getQueryPlan(true);
    }

    return block;
}

namespace
{
    struct RemoveSettings
    {
        using TypeToVisit = ASTSelectQuery;

        void visit(ASTSelectQuery & select_query, ASTPtr &) const
        {
            select_query.setExpression(ASTSelectQuery::Expression::SETTINGS, nullptr);
        }
    };

    using RemoveSettingsVisitor = InDepthNodeVisitor<OneTypeMatcher<RemoveSettings>, true>;

    class CollectPreparedParams
    {
    public:
        using TypeToVisit = ASTPreparedParameter;
        PreparedParameterSet prepared_params;

        void visit(ASTPreparedParameter & ast, ASTPtr &)
        {
            PreparedParameter param{ast.name, DataTypeFactory::instance().get(ast.type)};
            auto it = prepared_params.emplace(std::move(param));
            if (!it.second)
                throw Exception(ErrorCodes::BAD_PREPARED_PARAMETER, "Prepared param {} is duplicated", ast.name);
        }
    };

    using CollectPreparedParamsVisitor = InDepthNodeVisitor<OneTypeMatcher<CollectPreparedParams>, true>;
}

InterpreterSelectQueryUseOptimizer::InterpreterSelectQueryUseOptimizer(
    const ASTPtr & query_ptr_,
    PlanNodePtr sub_plan_ptr_,
    CTEInfo cte_info_,
    ContextMutablePtr & context_,
    const SelectQueryOptions & options_)
    : query_ptr(query_ptr_ ? query_ptr_->clone() : nullptr)
    , sub_plan_ptr(sub_plan_ptr_)
    , cte_info(std::move(cte_info_))
    , context(context_)
    , options(options_)
    , log(&Poco::Logger::get("InterpreterSelectQueryUseOptimizer"))
{
    interpret_sub_query = !!sub_plan_ptr;
}

QueryPlanPtr InterpreterSelectQueryUseOptimizer::getQueryPlan(bool skip_optimize)
{
    // When interpret sub query, reuse context info, e.g. PlanNodeIdAllocator, SymbolAllocator.
    if (interpret_sub_query)
    {
        QueryPlanPtr sub_query_plan = std::make_unique<QueryPlan>(sub_plan_ptr, cte_info, context->getPlanNodeIdAllocator());
        PlanOptimizer::optimize(*sub_query_plan, context);
        return sub_query_plan;
    }

    AnalysisPtr analysis;
    QueryPlanPtr query_plan;
    UInt128 query_hash;
    // not cache internal query
    bool enable_plan_cache = !options.is_internal && PlanCacheManager::enableCachePlan(query_ptr, context);
    // remove settings to avoid plan cache miss
    RemoveSettings remove_settings_data;
    RemoveSettingsVisitor(remove_settings_data).visit(query_ptr);

    if (const auto * execute_query = query_ptr->as<ASTExecutePreparedStatementQuery>())
    {
        auto * prepared_stat_manager = context->getPreparedStatementManager();
        if (!prepared_stat_manager)
            throw Exception("Prepared statement cache is not initialized", ErrorCodes::LOGICAL_ERROR);

        auto cache_result = prepared_stat_manager->getPlanFromCache(execute_query->name, context);
        query_plan = std::move(cache_result.plan);
        GraphvizPrinter::printLogicalPlan(*query_plan, context, "3997_get_prepared_plan");
        PreparedParameterBindings parameter_bindings;
        const auto * settings = execute_query->getValues()->as<const ASTSetQuery>();
        for (const auto & change : settings->changes)
        {
            if (auto it = cache_result.prepared_params.find(PreparedParameter{.name = change.name});
                it != cache_result.prepared_params.end())
                parameter_bindings.emplace(*it, change.value);
        }
        PreparedStatementContext prepare_context{std::move(parameter_bindings), context};
        query_plan->prepare(prepare_context);
        GraphvizPrinter::printLogicalPlan(*query_plan, context, "3998_prepare_query_plan");
    }
    else
    {
        if (enable_plan_cache)
        {
            context->applySettingChange({"enable_evaluate_constant_for_nondeterministic", false});
            if (context->getSettingsRef().enable_auto_prepared_statement)
            {
                SubstituteLiteralToPreparedParamsMatcher::Data substitude_prepared_param_data(context);
                SubstituteLiteralToPreparedParamsVisitor(substitude_prepared_param_data).visit(query_ptr);
                auto_prepared_params = std::move(substitude_prepared_param_data.extracted_binding);
                LOG_DEBUG(
                    log,
                    "extract auto prepared statement, SQL: {}, params: {}",
                    query_ptr->formatForErrorMessage(),
                    toString(auto_prepared_params));
            }

            query_hash = PlanCacheManager::hash(query_ptr, context);
            query_plan = PlanCacheManager::getPlanFromCache(query_hash, context);
            if (query_plan)
            {
                LOG_INFO(log, "hit plan cache");
                GraphvizPrinter::printLogicalPlan(*query_plan, context, "3996_get_plan_from_cache");
            }
            else if (context->getSettingsRef().force_plan_cache)
                throw Exception(ErrorCodes::PLAN_CACHE_NOT_USED, "plan cache not used");
        }

        if (!query_plan || context->getSettingsRef().iterative_optimizer_timeout == 999999)
        {
            buildQueryPlan(query_plan, analysis, skip_optimize);
            GraphvizPrinter::printLogicalPlan(*query_plan, context, "3997_build_plan_from_query");
            fillContextQueryAccessInfo(context, analysis);
            if (enable_plan_cache && query_hash && query_plan)
            {
                if (PlanCacheManager::addPlanToCache(query_hash, query_plan, analysis, context))
                    LOG_INFO(log, "plan cache added");
            }
        }

        if (!auto_prepared_params.empty())
        {
            query_plan->prepare(PreparedStatementContext{auto_prepared_params, context});
            GraphvizPrinter::printLogicalPlan(*query_plan, context, "3998_auto_prepare_query_plan");
        }
    }

    if (query_plan->getPlanNode())
        block = query_plan->getPlanNode()->getCurrentDataStream().header;
    GraphvizPrinter::printLogicalPlan(*query_plan, context, "3999_final_plan");
    query_plan->addInterpreterContext(context);
    LOG_DEBUG(log, "join order {}", JoinOrderUtils::getJoinOrder(*query_plan));
    return query_plan;
}

static void blockQueryJSONUseOptimizer(std::set<StorageID> used_storage_ids, ContextMutablePtr context)
{
    for (const auto & storage_id : used_storage_ids)
    {
        auto storage = DatabaseCatalog::instance().getTable(storage_id, context);
        if (storage->getInMemoryMetadataPtr()->hasDynamicSubcolumns())
            throw Exception("JSON query is not supported in Optimizer mode.", ErrorCodes::OPTIMIZER_NONSUPPORT);
    }
}

std::pair<PlanSegmentTreePtr, std::set<StorageID>> InterpreterSelectQueryUseOptimizer::getPlanSegment()
{
    Stopwatch stage_watch, total_watch;
    total_watch.start();
    setUnsupportedSettings(context);
    QueryPlanPtr query_plan = getQueryPlan();

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

    if (context->getSettingsRef().block_json_query_in_optimizer)
        blockQueryJSONUseOptimizer(used_storage_ids, context);
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
    ProfileEvents::increment(ProfileEvents::PlanSegmentSplitterTime, stage_watch.elapsedMilliseconds());

    resetFinalSampleSize(plan_segment_tree);
    setPlanSegmentInfoForExplainAnalyze(plan_segment_tree);
    GraphvizPrinter::printPlanSegment(plan_segment_tree, context);
    context->logOptimizerProfile(
        log, "Optimizer total run time: ", "Optimizer Total", std::to_string(total_watch.elapsedMillisecondsAsDouble()) + "ms");

    if (context->getSettingsRef().log_segment_profiles)
    {
        segment_profiles = std::make_shared<std::vector<String>>();
        for (auto & node : plan_segment_tree->getNodes())
            segment_profiles->emplace_back(
                PlanSegmentDescription::getPlanSegmentDescription(node.plan_segment, true)->jsonPlanSegmentDescriptionAsString({}));
    }

    return std::make_pair(std::move(plan_segment_tree), std::move(used_storage_ids));
}

QueryPipeline executeTEALimit(QueryPipeline & pipeline, ContextMutablePtr context, ASTPtr query_ptr, Poco::Logger * log)
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

        if (ast.list_of_selects->children.size() == 1)
        {
            auto & select_ast = ast.list_of_selects->children[0];

            if (auto * select = select_ast->as<ASTSelectQuery>())
            {
                if (select->orderBy())
                {
                    for (auto & elem : select->orderBy()->children)
                    {
                        if (comma)
                            postQuery << ", ";
                        comma = true;
                        auto new_elem = elem->clone();
                        new_elem->children.clear();
                        for (auto & child : elem->children)
                        {
                            if (auto * alias = dynamic_cast<ASTWithAlias *>(child.get()))
                            {
                                auto iden = std::make_shared<ASTIdentifier>(alias->getAliasOrColumnName());
                                new_elem->children.push_back(iden);
                            }
                        }
                        postQuery << serializeAST(*new_elem, true);
                    }
                }
            }
        }

        for (auto & elem : tea_limit->order_expr_list->children)
        {
            if (comma)
                postQuery << ", ";
            comma = true;
            postQuery << serializeAST(*elem, true);
        }
    }

    LOG_TRACE(log, "tealimit rewrited query with optimizer: {}", postQuery.str());
    context->applySettingsChanges({SettingChange("dialect_type", "CLICKHOUSE")});

    // evaluate the internal SQL and get the result
    return executeQuery(postQuery.str(), context->getQueryContext(), true).pipeline;
}

BlockIO InterpreterSelectQueryUseOptimizer::readFromQueryCache(ContextPtr local_context, QueryCacheContext & query_cache_context)
{
    auto query_cache = local_context->getQueryCache();
    const Settings & settings = local_context->getSettingsRef();

    if (!query_cache_context.can_use_query_cache || !settings.enable_reads_from_query_cache || !query_cache)
        return BlockIO{};

    auto query = query_ptr->clone();
    std::optional<std::set<StorageID>> used_storage_ids = getUsedStorageIds();
    TxnTimestamp & source_update_time_for_query_cache = query_cache_context.source_update_time_for_query_cache;
    query_cache_context.query_executed_by_optimizer = true;

    if (used_storage_ids.has_value() && !used_storage_ids->empty())
    {
        const std::set<StorageID> storage_ids = used_storage_ids.value();
        logUsedStorageIDs(log, storage_ids);
        if (settings.enable_transactional_query_cache)
            source_update_time_for_query_cache = getMaxUpdateTime(storage_ids, context);
        else
            source_update_time_for_query_cache = TxnTimestamp::minTS();

        LOG_DEBUG(
            log,
            "max update timestamp {}, txn_id {}",
            source_update_time_for_query_cache,
            local_context->getCurrentTransactionID().toUInt64());
        if ((settings.enable_transactional_query_cache == false) || (source_update_time_for_query_cache.toUInt64() != 0))
        {
            QueryCache::Key key(
                query,
                block,
                local_context->getUserName(),
                /*dummy for is_shared*/ false,
                /*dummy value for expires_at*/ std::chrono::system_clock::from_time_t(1),
                /*dummy value for is_compressed*/ false,
                local_context->getCurrentTransactionID());
            QueryCache::Reader reader = query_cache->createReader(key, source_update_time_for_query_cache);
            if (reader.hasCacheEntryForKey())
            {
                QueryPipeline pipeline;
                pipeline.readFromQueryCache(reader.getSource(), reader.getSourceTotals(), reader.getSourceExtremes());
                BlockIO res;
                res.pipeline = std::move(pipeline);
                query_cache_context.query_cache_usage = QueryCache::Usage::Read;
                return res;
            }
        }
    }

    return BlockIO{};
}

BlockIO InterpreterSelectQueryUseOptimizer::execute()
{
    if (query_ptr && query_ptr->as<ASTCreatePreparedStatementQuery>())
    {
        // if (!create_prepared->cluster.empty())
        //     return executeDDLQueryOnCluster(query_ptr, context);
        return executeCreatePreparedStatementQuery();
    }
    if (!plan_segment_tree_ptr)
    {
        std::pair<PlanSegmentTreePtr, std::set<StorageID>> plan_segment_tree_and_used_storage_ids = getPlanSegment();
        plan_segment_tree_ptr = std::move(plan_segment_tree_and_used_storage_ids.first);
    }
    size_t plan_segment_num = plan_segment_tree_ptr->getNodes().size();

    UInt64 max_plan_segment_num = context->getSettingsRef().max_plan_segment_num;
    if (max_plan_segment_num != 0 && plan_segment_num > max_plan_segment_num)
        throw Exception(
            fmt::format(
                "query_id:{} plan_segments size {} exceed max_plan_segment_num {}",
                context->getCurrentQueryId(),
                plan_segment_num,
                max_plan_segment_num),
            ErrorCodes::TOO_MANY_PLAN_SEGMENTS);

    auto coodinator = std::make_shared<MPPQueryCoordinator>(std::move(plan_segment_tree_ptr), context, MPPQueryOptions());

    BlockIO res = coodinator->execute();

    auto select_union = query_ptr ? query_ptr->as<ASTSelectWithUnionQuery>() : nullptr;
    if (select_union)
    {
        if (unlikely(select_union->tealimit))
            res.pipeline = executeTEALimit(res.pipeline, context, query_ptr, log);
    }
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

void InterpreterSelectQueryUseOptimizer::resetFinalSampleSize(PlanSegmentTreePtr & plan_segment_tree)
{
    for (auto & plan_segment : plan_segment_tree->getNodes())
    {
        if (plan_segment.getPlanSegment())
        {
            for (auto & node : plan_segment.getPlanSegment()->getQueryPlan().getNodes())
            {
                if (auto * sample = dynamic_cast<FinalSampleStep *>(node.step.get()))
                {
                    size_t sample_size = (sample->getSampleSize() + 1) / plan_segment.getPlanSegment()->getParallelSize();
                    sample->setSampleSize(sample_size);
                }
            }
        }
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
                backQuoteIfNeed(storage_id.getDatabaseName()), storage_id.getFullTableName(), required_columns);
        }
    }
}

std::optional<std::set<StorageID>> InterpreterSelectQueryUseOptimizer::getUsedStorageIds()
{
    if (plan_segment_tree_ptr)
    {
        throw Exception("Cannot call this getUsedStorageIds twice", ErrorCodes::LOGICAL_ERROR);
    }

    std::pair<PlanSegmentTreePtr, std::set<StorageID>> plan_segment_tree_and_used_storage_ids = getPlanSegment();

    plan_segment_tree_ptr = std::move(plan_segment_tree_and_used_storage_ids.first);
    return std::optional<std::set<StorageID>>(std::move(plan_segment_tree_and_used_storage_ids.second));
}

void InterpreterSelectQueryUseOptimizer::setUnsupportedSettings(ContextMutablePtr & context)
{
    if (!context->getSettingsRef().enable_optimizer)
        return;

    SettingsChanges setting_changes;
    setting_changes.emplace_back("distributed_aggregation_memory_efficient", false);

    context->applySettingsChanges(setting_changes);
}

void InterpreterSelectQueryUseOptimizer::fillQueryPlan(ContextPtr context, QueryPlan & query_plan)
{
    WriteBufferFromOwnString buffer;
    Protos::QueryPlan plan_pb;
    query_plan.toProto(plan_pb);
    String json_msg;
    google::protobuf::util::JsonPrintOptions pb_options;
    pb_options.preserve_proto_field_names = true;
    pb_options.always_print_primitive_fields = true;
    pb_options.add_whitespace = false;

    google::protobuf::util::MessageToJsonString(plan_pb, &json_msg, pb_options);
    buffer << json_msg;

    context->getQueryContext()->addQueryPlanInfo(buffer.str());
}

void InterpreterSelectQueryUseOptimizer::buildQueryPlan(QueryPlanPtr & query_plan, AnalysisPtr & analysis, bool skip_optimize)
{
    context->createPlanNodeIdAllocator();
    context->createSymbolAllocator();
    context->createOptimizerMetrics();

    Stopwatch stage_watch;
    stage_watch.start();
    query_ptr = QueryRewriter().rewrite(query_ptr, context);
    context->logOptimizerProfile(
        log, "Optimizer stage run time: ", "Rewrite", std::to_string(stage_watch.elapsedMillisecondsAsDouble()) + "ms");
    ProfileEvents::increment(ProfileEvents::QueryRewriterTime, stage_watch.elapsedMilliseconds());

    stage_watch.restart();
    analysis = QueryAnalyzer::analyze(query_ptr, context);
    fillContextQueryAccessInfo(context, analysis);
    context->logOptimizerProfile(
        log, "Optimizer stage run time: ", "Analyzer", std::to_string(stage_watch.elapsedMillisecondsAsDouble()) + "ms");
    ProfileEvents::increment(ProfileEvents::QueryAnalyzerTime, stage_watch.elapsedMilliseconds());

    stage_watch.restart();
    query_plan = QueryPlanner().plan(query_ptr, *analysis, context);
    context->logOptimizerProfile(
        log, "Optimizer stage run time: ", "Planning", std::to_string(stage_watch.elapsedMillisecondsAsDouble()) + "ms");
    ProfileEvents::increment(ProfileEvents::QueryPlannerTime, stage_watch.elapsedMilliseconds());

    if (!skip_optimize)
    {
        stage_watch.restart();
        PlanOptimizer::optimize(*query_plan, context);

        if (context->getSettingsRef().log_query_plan)
        {
            fillQueryPlan(context, *query_plan);
        }

        context->logOptimizerProfile(
            log, "Optimizer stage run time: ", "Optimizer", std::to_string(stage_watch.elapsedMillisecondsAsDouble()) + "ms");
        ProfileEvents::increment(ProfileEvents::QueryOptimizerTime, stage_watch.elapsedMilliseconds());
    }
}

BlockIO InterpreterSelectQueryUseOptimizer::executeCreatePreparedStatementQuery()
{
    const auto & prepare = query_ptr->as<const ASTCreatePreparedStatementQuery &>();
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_PREPARED_STATEMENT);

    if (prepare.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_PREPARED_STATEMENT);
    context->checkAccess(access_rights_elements);

    auto * prep_stat_manager = context->getPreparedStatementManager();
    if (!prep_stat_manager)
        throw Exception("Prepare cache has to be initialized", ErrorCodes::LOGICAL_ERROR);

    AddDefaultDatabaseVisitor add_default_db_visitor(context, context->getCurrentDatabase());
    add_default_db_visitor.visit(query_ptr);

    String name;
    String formatted_query;
    SettingsChanges settings_changes;
    ASTPtr prepare_ast = query_ptr->clone();
    {
        name = prepare.getName();
        settings_changes = InterpreterSetQuery::extractSettingsFromQuery(query_ptr, context);
    }

    QueryPlanPtr query_plan;
    AnalysisPtr analysis;
    buildQueryPlan(query_plan, analysis);
    CollectPreparedParams prepared_params_collector;
    CollectPreparedParamsVisitor(prepared_params_collector).visit(query_ptr);
    prep_stat_manager->addPlanToCache(
        name, prepare_ast, settings_changes, query_plan, analysis, std::move(prepared_params_collector.prepared_params), context);
    return {};
}

bool InterpreterSelectQueryUseOptimizer::isCreatePreparedStatement()
{
    return query_ptr->as<ASTCreatePreparedStatementQuery>();
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
            .shard_number = worker_group->getShardsInfo().size(),
            .cluster_name = worker_group->getID(),
            .plan_segment_tree = cluster_info_context.plan_segment_tree.get(),
            .health_parallel
            = worker_group_status_ptr ? std::optional<size_t>(worker_group_status_ptr->getAvaiableComputeWorkerSize()) : std::nullopt};

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
    PlanSegmentDescriptions plan_segment_descriptions;
    bool record_plan_detail = explain->getSetting().json && (explain->getKind() != ASTExplainQuery::ExplainKind::PipelineAnalyze);
    for (auto & segment_node : nodes)
    {
        if (explain->getKind() == ASTExplainQuery::ExplainKind::DistributedAnalyze
            || explain->getKind() == ASTExplainQuery::ExplainKind::LogicalAnalyze)
            segment_node.plan_segment->setProfileType(ReportProfileType::QueryPlan);
        else if (explain->getKind() == ASTExplainQuery::ExplainKind::PipelineAnalyze)
            segment_node.plan_segment->setProfileType(ReportProfileType::QueryPipeline);

        if (explain->getKind() == ASTExplainQuery::ExplainKind::DistributedAnalyze
            || explain->getKind() == ASTExplainQuery::ExplainKind::PipelineAnalyze)
            plan_segment_descriptions.emplace_back(
                PlanSegmentDescription::getPlanSegmentDescription(segment_node.plan_segment, record_plan_detail));
    }

    explain->setPlanSegmentDescriptions(plan_segment_descriptions);
}

void ExplainAnalyzeVisitor::visitNode(QueryPlan::Node * node, PlanSegmentTree::Nodes & nodes)
{
    for (const auto & child : node->children)
        VisitorUtil::accept(child, *this, nodes);
}

}
