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

#include <Storages/StorageCloudMergeTree.h>

#include <Common/Exception.h>
#include "Core/UUID.h"
#include "Storages/IStorage.h"
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <MergeTreeCommon/CnchBucketTableCommon.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/ReadFromPreparedSource.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/CloudMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MutationCommands.h>
#include <WorkerTasks/CloudMergeTreeMutateTask.h>
#include <WorkerTasks/CloudMergeTreeMergeTask.h>
#include <WorkerTasks/CloudMergeTreeReclusterTask.h>
#include <WorkerTasks/CloudUniqueMergeTreeMergeTask.h>
#include <WorkerTasks/ManipulationTaskParams.h>
#include <WorkerTasks/ManipulationType.h>
#include <CloudServices/CloudMergeTreeDedupWorker.h>
#include <CloudServices/CnchPartsHelper.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/StorageReplicatedMergeTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

StorageCloudMergeTree::StorageCloudMergeTree(
    const StorageID & table_id_,
    String cnch_database_name_,
    String cnch_table_name_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
    const String & date_column_name_,
    const MergeTreeMetaBase::MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_)
    : MergeTreeCloudData( // NOLINT
        table_id_,
        metadata_,
        context_,
        date_column_name_,
        merging_params_,
        std::move(settings_))
    , cnch_database_name(std::move(cnch_database_name_))
    , cnch_table_name(std::move(cnch_table_name_))
{
    setServerVwName(getSettings()->cnch_server_vw);
    setInMemoryMetadata(metadata_);
    format_version = MERGE_TREE_CHCH_DATA_STORAGTE_VERSION;

    String cnch_uuid = UUIDHelpers::UUIDToString(getCnchStorageUUID());
    String relative_table_path = getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk()->getTableRelativePathOnDisk(cnch_uuid);
    MergeTreeMetaBase::setRelativeDataPath(IStorage::StorageLocation::MAIN, relative_table_path);
    relative_auxility_storage_path = fs::path("auxility_store") / relative_table_path / "";

    if (getInMemoryMetadataPtr()->hasUniqueKey() && getSettings()->cloud_enable_dedup_worker)
        dedup_worker = std::make_unique<CloudMergeTreeDedupWorker>(*this);
}

void StorageCloudMergeTree::startup()
{
    if (dedup_worker)
        dedup_worker->start();
}

void StorageCloudMergeTree::shutdown()
{
    if (dedup_worker)
        dedup_worker->stop();
}

StorageCloudMergeTree::~StorageCloudMergeTree()
{
}

void StorageCloudMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    if (data_version)
        prepareVersionedPartsForRead(local_context, query_info, column_names);

    if (auto plan = MergeTreeDataSelectExecutor(*this).read(
            column_names, storage_snapshot, query_info, local_context, max_block_size, num_streams, processed_stage))
        query_plan = std::move(*plan);
}

Pipe StorageCloudMergeTree::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(
        QueryPlanOptimizationSettings::fromContext(local_context), BuildQueryPipelineSettings::fromContext(local_context));
}

BlockOutputStreamPtr StorageCloudMergeTree::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    ASTPtr overwrite_partition;
    if (query)
    {
        if (auto * insert_query = dynamic_cast<ASTInsertQuery *>(query.get()))
            overwrite_partition = insert_query->is_overwrite ? insert_query->overwrite_partition : nullptr;
    }
    return std::make_shared<CloudMergeTreeBlockOutputStream>(*this, metadata_snapshot, std::move(local_context), overwrite_partition);
}

ManipulationTaskPtr StorageCloudMergeTree::manipulate(const ManipulationTaskParams & input_params, ContextPtr task_context)
{
    ManipulationTaskPtr task;
    switch (input_params.type)
    {
        case ManipulationType::Merge:
            if (getInMemoryMetadataPtr()->hasUniqueKey())
                task = std::make_shared<CloudUniqueMergeTreeMergeTask>(*this, input_params, task_context);
            else
                task = std::make_shared<CloudMergeTreeMergeTask>(*this, input_params, task_context);
            break;
        case ManipulationType::Mutate:
            task = std::make_shared<CloudMergeTreeMutateTask>(*this, input_params, task_context);
            break;
        case ManipulationType::Clustering:
            task = std::make_shared<CloudMergeTreeReclusterTask>(*this, input_params, task_context);
            break;
        default:
            throw Exception("Unsupported manipulation task: " + String(typeToString(input_params.type)), ErrorCodes::NOT_IMPLEMENTED);
    }
    return task;
}

void StorageCloudMergeTree::checkMutationIsPossible(const MutationCommands & commands, const Settings & /*settings*/) const
{
    for (const auto & command : commands)
    {
        if (command.type == MutationCommand::Type::MATERIALIZE_TTL)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "It's not allowed to execute MATERIALIZE_TTL commands");
    }
}

void StorageCloudMergeTree::checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr &, const Settings &) const
{
    for (const auto & command : commands)
    {
        if (command.type != PartitionCommand::Type::INGEST_PARTITION)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "It's not allowed to execute any PARTITION commands except INGEST_PARTITON");
    }
}

Pipe StorageCloudMergeTree::alterPartition(
    const StorageMetadataPtr & metadata_snapshot,
    const PartitionCommands & commands,
    ContextPtr local_context,
    const ASTPtr & /* query */)
{
    if (commands.size() > 1U)
        throw Exception(
            ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Too many commands in the same alter partition query on storage {}", getName());
    else if (commands.empty())
        return {};

    const auto & command = commands.at(0U);
    switch (command.type)
    {
        case PartitionCommand::INGEST_PARTITION:
            return ingestPartition(metadata_snapshot, command, std::move(local_context));
        default:
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Partition command {} are not supported by storage {}", command.typeToString(), getName());
    }
}

bool StorageCloudMergeTree::checkStagedParts()
{
    if (!getInMemoryMetadataPtr()->hasUniqueKey())
        return true;

    auto catalog = getContext()->getCnchCatalog();
    auto ts = getContext()->getTimestamp();
    auto cnch_table = catalog->tryGetTableByUUID(*getContext(), UUIDHelpers::UUIDToString(getStorageUUID()), ts);

    /// get all the partitions of committed staged parts
    auto staged_parts = catalog->getStagedServerDataParts(cnch_table, ts);
    staged_parts = CnchPartsHelper::calcVisibleParts(staged_parts, /*collect_on_chain*/ false);
    size_t num_to_publish = 0;
    for (auto & part : staged_parts)
    {
        if (TxnTimestamp(ts).toMillisecond() - TxnTimestamp(part->getCommitTime()).toMillisecond()
            > getSettings()->staged_part_lifetime_threshold_ms_to_block_kafka_consume)
        {
            num_to_publish++;
            LOG_DEBUG(
                log,
                "The part: {}, commit time: {} ms, current time: {} ms, staged_part_lifetime_threshold_ms_to_block_kafka_consume: {} ms.",
                part->name(),
                TxnTimestamp(part->getCommitTime()).toMillisecond(),
                TxnTimestamp(ts).toMillisecond(),
                getSettings()->staged_part_lifetime_threshold_ms_to_block_kafka_consume);
        }
    }
    if (num_to_publish == 0)
        return true;
    LOG_DEBUG(
        log,
        "There are {} parts need to be published which are commited before {} ms ago.",
        staged_parts.size() << getSettings()->staged_part_lifetime_threshold_ms_to_block_kafka_consume);
    return false;
}

CloudMergeTreeDedupWorker * StorageCloudMergeTree::getDedupWorker()
{
    if (!dedup_worker)
        throw Exception("DedupWorker is not created", ErrorCodes::LOGICAL_ERROR);
    return dedup_worker.get();
}

static UInt64 getDataVersion(const std::shared_ptr<const IMergeTreeDataPart> & part)
{
    return std::max(
        std::max(part->columns_commit_time.toUInt64(), part->mutation_commit_time.toUInt64()),
        static_cast<UInt64>(part->info.min_block));
}

MutationCommands StorageCloudMergeTree::getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const
{
    auto data_version = getDataVersion(part);
    std::lock_guard lock(mutations_by_version_mutex);
    auto it = mutations_by_version.upper_bound(data_version);
    if (it == mutations_by_version.end())
        return {};

    return it->second.commands;
}

StoragePolicyPtr StorageCloudMergeTree::getStoragePolicy(StorageLocation location) const
{
    if (location == StorageLocation::MAIN)
    {
        if (getSettings()->enable_cloudfs)
        {
            /// This will be used for compatibility with old version of Cross (bundled with DiskByteHDFS)
            const String & storage_policy_name = getSettings()->storage_policy.value + CLOUDFS_STORAGE_POLICY_SUFFIX;
            auto policy = getContext()->tryGetStoragePolicy(storage_policy_name);
            if (policy)
                return policy;
            else
                LOG_WARNING(log, "Storage Policy {} is not found and will fallback to use ufs storage policy", storage_policy_name);
        }

        return getContext()->getStoragePolicy(getSettings()->storage_policy);
    }
    else
        return getContext()->getStoragePolicy(getContext()->getCnchAuxilityPolicyName());
}

const String& StorageCloudMergeTree::getRelativeDataPath(StorageLocation location) const
{
    return location == StorageLocation::MAIN ?
        MergeTreeMetaBase::getRelativeDataPath(location) :
        relative_auxility_storage_path;
}

ASTs StorageCloudMergeTree::convertBucketNumbersToAstLiterals(const ASTPtr where_expression, ContextPtr local_context) const
{
    ASTs result;
    ASTPtr where_expression_copy = where_expression->clone();
    const Settings & settings = local_context->getSettingsRef();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    if (settings.optimize_skip_unused_shards && where_expression && isBucketTable() && metadata_snapshot->getColumnsForClusterByKey().size() == 1 && !required_bucket_numbers.empty())
    {

        Block sample_block = metadata_snapshot->getSampleBlock();
        NamesAndTypesList source_columns = sample_block.getNamesAndTypesList();

        auto syntax_result = TreeRewriter(local_context).analyze(where_expression_copy, source_columns);
        ExpressionActionsPtr const_actions = ExpressionAnalyzer{where_expression_copy, syntax_result, local_context}.getConstActions();

        // get required source columns
        Names required_source_columns = syntax_result->requiredSourceColumns();
        NameSet required_source_columns_set = NameSet(required_source_columns.begin(), required_source_columns.end());

        // Delete all columns that are not required
        for (const auto & delete_column : sample_block.getNamesAndTypesList())
        {
            if (!required_source_columns_set.contains(delete_column.name))
            {
                sample_block.erase(delete_column.name);
            }
        }

        const_actions->execute(sample_block);

        //replace constant values as literals in AST using visitor
        if (sample_block)
        {
            InDepthNodeVisitor<ReplacingConstantExpressionsMatcher, true> visitor(sample_block);
            visitor.visit(where_expression_copy);
        }

        // Increment limit so that when limit reaches 0, it means that the limit has been exceeded
        size_t limit = settings.optimize_skip_unused_shards_limit + 1;

        const auto & blocks = evaluateExpressionOverConstantCondition(where_expression_copy, metadata_snapshot->getClusterByKey().expression, limit);

        if (!limit)
        {
            LOG_INFO(
                log,
                "Number of values for cluster_by key exceeds optimize_skip_unused_shards_limit = "
                    + std::to_string(settings.optimize_skip_unused_shards_limit)
                    + ", try to increase it, but note that this may increase query processing time.");
        }
        LOG_TRACE(
                log,
                "StorageCloudMergeTree::convertBucketNumbersToAstLiterals blocks.size() = "
                    + std::to_string(blocks->size()));
        if (blocks)
        {
            for (const auto & block : *blocks)
            {
                // Get bucket number of this single value from the IN set
                Block block_copy = block;
                prepareBucketColumn(block_copy, metadata_snapshot->getColumnsForClusterByKey(), metadata_snapshot->getSplitNumberFromClusterByKey(), metadata_snapshot->getWithRangeFromClusterByKey(), metadata_snapshot->getBucketNumberFromClusterByKey(), local_context, metadata_snapshot->getIsUserDefinedExpressionFromClusterByKey());
                auto bucket_number = block_copy.getByPosition(block_copy.columns() - 1).column->getInt(0); // this block only contains one row

                // Create ASTLiteral using the bucket column in the block if it can be found in required_bucket_numbers
                if (settings.optimize_skip_unused_shards_rewrite_in && required_bucket_numbers.contains(bucket_number))
                {
                    const ColumnWithTypeAndName & col = block_copy.getByName(metadata_snapshot->getColumnsForClusterByKey()[0]);
                    Field field;
                    col.column->get(0, field);
                    auto ast = std::make_shared<ASTLiteral>(field);
                    result.push_back(ast);
                }
            }
        }
    }
    return result;
}

QueryProcessingStage::Enum StorageCloudMergeTree::getQueryProcessingStage(
    ContextPtr query_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info) const
{
    if (to_stage >= QueryProcessingStage::Enum::WithMergeableState)
    {
        if (getQueryProcessingStageWithAggregateProjection(query_context, storage_snapshot, query_info))
        {
            if (query_info.projection->desc->type == ProjectionDescription::Type::Aggregate)
                return QueryProcessingStage::Enum::WithMergeableState;
        }
    }

    return QueryProcessingStage::Enum::FetchColumns;
}

static void selectBestProjection(
    const MergeTreeDataSelectExecutor & reader,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Names & required_columns,
    ProjectionCandidate & candidate,
    ContextPtr query_context,
    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks,
    const Settings & settings,
    const MergeTreeData::DataPartsVector & parts,
    ProjectionCandidate *& selected_candidate,
    size_t & min_sum_marks)
{
    MergeTreeData::DataPartsVector projection_parts;
    MergeTreeData::DataPartsVector normal_parts;
    for (const auto & part : parts)
    {
        const auto & projections = part->getProjectionParts();
        auto it = projections.find(candidate.desc->name);
        if (it != projections.end())
            projection_parts.push_back(it->second);
        else
            normal_parts.push_back(part);
    }

    if (projection_parts.empty())
        return;

    auto sum_marks = reader.estimateNumMarksToRead(
        projection_parts,
        candidate.required_columns,
        metadata_snapshot,
        candidate.desc->metadata,
        query_info, // TODO syntax_analysis_result set in index
        query_context,
        settings.max_threads,
        max_added_blocks)->marks();

    if (normal_parts.empty())
    {
        // All parts are projection parts which allows us to use in_order_optimization.
        // TODO It might be better to use a complete projection even with more marks to read.
        candidate.complete = true;
    }
    else
    {
        sum_marks += reader.estimateNumMarksToRead(
            normal_parts,
            required_columns,
            metadata_snapshot,
            metadata_snapshot,
            query_info, // TODO syntax_analysis_result set in index
            query_context,
            settings.max_threads,
            max_added_blocks)->marks();
    }

    // We choose the projection with least sum_marks to read.
    if (sum_marks < min_sum_marks)
    {
        selected_candidate = &candidate;
        min_sum_marks = sum_marks;
    }
}

bool StorageCloudMergeTree::getQueryProcessingStageWithAggregateProjection(
    ContextPtr query_context, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo & query_info) const
{
    const auto & metadata_snapshot = storage_snapshot->metadata;
    const auto & settings = query_context->getSettingsRef();
    if (!settings.allow_experimental_projection_optimization || query_info.ignore_projections || query_info.is_projection_query)
        return false;

    const auto & query_ptr = query_info.query;

    // Currently projections don't support final yet.
    if (auto * select = query_ptr->as<ASTSelectQuery>(); select && select->final())
        return false;

    InterpreterSelectQuery select(
        query_ptr, query_context, SelectQueryOptions{QueryProcessingStage::WithMergeableState}.ignoreProjections().ignoreAlias());
    const auto & analysis_result = select.getAnalysisResult();

    bool can_use_aggregate_projection = true;
    /// If the first stage of the query pipeline is more complex than Aggregating - Expression - Filter - ReadFromStorage,
    /// we cannot use aggregate projection.
    if (analysis_result.join != nullptr || analysis_result.array_join != nullptr)
        can_use_aggregate_projection = false;

    /// Check if all needed columns can be provided by some aggregate projection. Here we also try
    /// to find expression matches. For example, suppose an aggregate projection contains a column
    /// named sum(x) and the given query also has an expression called sum(x), it's a match. This is
    /// why we need to ignore all aliases during projection creation and the above query planning.
    /// It's also worth noting that, sqrt(sum(x)) will also work because we can treat sum(x) as a
    /// required column.

    /// The ownership of ProjectionDescription is hold in metadata_snapshot which lives along with
    /// InterpreterSelect, thus we can store the raw pointer here.
    std::vector<ProjectionCandidate> candidates;
    NameSet keys;
    std::unordered_map<std::string_view, size_t> key_name_pos_map;
    size_t pos = 0;
    for (const auto & desc : select.getQueryAnalyzer()->aggregationKeys())
    {
        keys.insert(desc.name);
        key_name_pos_map.insert({desc.name, pos++});
    }
    auto actions_settings = ExpressionActionsSettings::fromSettings(settings);

    // All required columns should be provided by either current projection or previous actions
    // Let's traverse backward to finish the check.
    // TODO what if there is a column with name sum(x) and an aggregate sum(x)?
    auto rewrite_before_where =
        [&](ProjectionCandidate & candidate, const ProjectionDescription & projection,
            NameSet & required_columns, const Block & source_block, const Block & aggregates)
    {
        if (analysis_result.before_where)
        {
            candidate.where_column_name = analysis_result.where_column_name;
            candidate.remove_where_filter = analysis_result.remove_where_filter;
            candidate.before_where = analysis_result.before_where->clone();

            required_columns = candidate.before_where->foldActionsByProjection(
                required_columns,
                projection.sample_block_for_keys,
                candidate.where_column_name);

            if (required_columns.empty())
                return false;
            candidate.before_where->addAggregatesViaProjection(aggregates);
        }

        if (analysis_result.prewhere_info)
        {
            candidate.prewhere_info = analysis_result.prewhere_info;

            auto prewhere_actions = candidate.prewhere_info->prewhere_actions->clone();
            auto prewhere_required_columns = required_columns;
            // required_columns should not contain columns generated by prewhere
            for (const auto & column : prewhere_actions->getResultColumns())
                required_columns.erase(column.name);

            // Prewhere_action should not add missing keys.
            prewhere_required_columns = prewhere_actions->foldActionsByProjection(
                prewhere_required_columns, projection.sample_block_for_keys, candidate.prewhere_info->prewhere_column_name, false);

            if (prewhere_required_columns.empty())
                return false;
            candidate.prewhere_info->prewhere_actions = prewhere_actions;

            if (candidate.prewhere_info->row_level_filter)
            {
                auto row_level_filter_actions = candidate.prewhere_info->row_level_filter->clone();
                prewhere_required_columns = row_level_filter_actions->foldActionsByProjection(
                    prewhere_required_columns, projection.sample_block_for_keys, candidate.prewhere_info->row_level_column_name, false);

                if (prewhere_required_columns.empty())
                    return false;
                candidate.prewhere_info->row_level_filter = row_level_filter_actions;
            }

            if (candidate.prewhere_info->alias_actions)
            {
                auto alias_actions = candidate.prewhere_info->alias_actions->clone();
                prewhere_required_columns
                    = alias_actions->foldActionsByProjection(prewhere_required_columns, projection.sample_block_for_keys, {}, false);

                if (prewhere_required_columns.empty())
                    return false;
                candidate.prewhere_info->alias_actions = alias_actions;
            }
            required_columns.insert(prewhere_required_columns.begin(), prewhere_required_columns.end());
        }

        bool match = true;
        for (const auto & column : required_columns)
        {
            /// There are still missing columns, fail to match
            if (!source_block.has(column))
            {
                match = false;
                break;
            }
        }
        return match;
    };

    for (const auto & projection : metadata_snapshot->projections)
    {
        ProjectionCandidate candidate{};
        candidate.desc = &projection;

        if (projection.type == ProjectionDescription::Type::Aggregate && analysis_result.need_aggregate && can_use_aggregate_projection)
        {
            bool match = true;
            Block aggregates;
            // Let's first check if all aggregates are provided by current projection
            for (const auto & aggregate : select.getQueryAnalyzer()->aggregates())
            {
                const auto * column = projection.sample_block.findByName(aggregate.column_name);
                if (column)
                {
                    aggregates.insert(*column);
                }
                else
                {
                    match = false;
                    break;
                }
            }

            if (!match)
                continue;

            // Check if all aggregation keys can be either provided by some action, or by current
            // projection directly. Reshape the `before_aggregation` action DAG so that it only
            // needs to provide aggregation keys, and certain children DAG might be substituted by
            // some keys in projection.
            candidate.before_aggregation = analysis_result.before_aggregation->clone();
            auto required_columns = candidate.before_aggregation->foldActionsByProjection(keys, projection.sample_block_for_keys);

            if (required_columns.empty() && !keys.empty())
                continue;

            if (analysis_result.optimize_aggregation_in_order)
            {
                for (const auto & key : keys)
                {
                    auto actions_dag = analysis_result.before_aggregation->clone();
                    actions_dag->foldActionsByProjection({key}, projection.sample_block_for_keys);
                    candidate.group_by_elements_actions.emplace_back(std::make_shared<ExpressionActions>(actions_dag, actions_settings));
                }
            }

            // Reorder aggregation keys and attach aggregates
            candidate.before_aggregation->reorderAggregationKeysForProjection(key_name_pos_map);
            candidate.before_aggregation->addAggregatesViaProjection(aggregates);

            if (rewrite_before_where(candidate, projection, required_columns, projection.sample_block_for_keys, aggregates))
            {
                candidate.required_columns = {required_columns.begin(), required_columns.end()};
                for (const auto & aggregate : aggregates)
                    candidate.required_columns.push_back(aggregate.name);
                candidates.push_back(std::move(candidate));
            }
        }

        if (projection.type == ProjectionDescription::Type::Normal && (analysis_result.hasWhere() || analysis_result.hasPrewhere()))
        {
            const auto & actions
                = analysis_result.before_aggregation ? analysis_result.before_aggregation : analysis_result.before_order_by;
            NameSet required_columns;
            for (const auto & column : actions->getRequiredColumns())
                required_columns.insert(column.name);

            if (rewrite_before_where(candidate, projection, required_columns, projection.sample_block, {}))
            {
                candidate.required_columns = {required_columns.begin(), required_columns.end()};
                candidates.push_back(std::move(candidate));
            }
        }
    }

    // Let's select the best projection to execute the query.
    if (!candidates.empty())
    {
        std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks;
        // if (settings.select_sequential_consistency)
        // {
        //     if (const StorageReplicatedMergeTree * replicated = dynamic_cast<const StorageReplicatedMergeTree *>(this))
        //         max_added_blocks = std::make_shared<PartitionIdToMaxBlock>(replicated->getMaxAddedBlocks());
        // }

        auto parts = getDataPartsVector();
        MergeTreeDataSelectExecutor reader(*this);

        ProjectionCandidate * selected_candidate = nullptr;
        size_t min_sum_marks = std::numeric_limits<size_t>::max();
        bool has_ordinary_projection = false;
        /// Favor aggregate projections
        for (auto & candidate : candidates)
        {
            if (candidate.desc->type == ProjectionDescription::Type::Aggregate)
            {
                selectBestProjection(
                    reader,
                    metadata_snapshot,
                    query_info,
                    analysis_result.required_columns,
                    candidate,
                    query_context,
                    max_added_blocks,
                    settings,
                    parts,
                    selected_candidate,
                    min_sum_marks);
            }
            else
                has_ordinary_projection = true;
        }

        /// Select the best normal projection if no aggregate projection is available
        if (!selected_candidate && has_ordinary_projection)
        {
            min_sum_marks = reader.estimateNumMarksToRead(
                parts,
                analysis_result.required_columns,
                metadata_snapshot,
                metadata_snapshot,
                query_info, // TODO syntax_analysis_result set in index
                query_context,
                settings.max_threads,
                max_added_blocks)->marks();

            // Add 1 to base sum_marks so that we prefer projections even when they have equal number of marks to read.
            // NOTE: It is not clear if we need it. E.g. projections do not support skip index for now.
            min_sum_marks += 1;

            for (auto & candidate : candidates)
            {
                if (candidate.desc->type == ProjectionDescription::Type::Normal)
                {
                    selectBestProjection(
                        reader,
                        metadata_snapshot,
                        query_info,
                        analysis_result.required_columns,
                        candidate,
                        query_context,
                        max_added_blocks,
                        settings,
                        parts,
                        selected_candidate,
                        min_sum_marks);
                }
            }
        }

        if (!selected_candidate)
            return false;

        if (selected_candidate->desc->type == ProjectionDescription::Type::Aggregate)
        {
            selected_candidate->aggregation_keys = select.getQueryAnalyzer()->aggregationKeys();
            selected_candidate->aggregate_descriptions = select.getQueryAnalyzer()->aggregates();
        }

        query_info.projection = std::move(*selected_candidate);

        return true;
    }
    return false;
}

std::unique_ptr<MergeTreeSettings> StorageCloudMergeTree::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
}

}
