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

#pragma once
#include <Common/Logger.h>
#include <Core/QueryProcessingStage.h>
#include <Core/SortDescription.h>
#include <Interpreters/SelectQueryOptions.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/ISourceStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <QueryPlan/QueryPlan.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class TableScanStep : public ISourceStep
{
public:
    // Server
    TableScanStep(
        ContextPtr context,
        StorageID storage_id_,
        const NamesWithAliases & column_alias_,
        const SelectQueryInfo & query_info_,
        size_t max_block_size_,
        String alias_ = "",
        bool bucket_scan_ = false,
        PlanHints hints_ = {},
        Assignments inline_expressions_ = {},
        std::shared_ptr<AggregatingStep> aggregation_ = nullptr,
        std::shared_ptr<ProjectionStep> projection_ = nullptr,
        std::shared_ptr<FilterStep> filter_ = nullptr);

    // Worker
    TableScanStep(
        ContextPtr context,
        DataStream output_stream_,
        StorageID storage_id_,
        NamesWithAliases column_alias_,
        SelectQueryInfo query_info_,
        size_t max_block_size_,
        String alias_,
        PlanHints hints_,
        Assignments inline_expressions_,
        std::shared_ptr<AggregatingStep> aggregation_,
        std::shared_ptr<ProjectionStep> projection_,
        std::shared_ptr<FilterStep> filter_,
        DataStream table_output_stream_);

    // Copy
    TableScanStep(
        DataStream output,
        StoragePtr storage_,
        StorageID storage_id_,
        StorageMetadataPtr metadata_snapshot_,
        StorageSnapshotPtr storage_snapshot_,
        String original_table_,
        Names column_names_,
        NamesWithAliases column_alias_,
        SelectQueryInfo query_info_,
        size_t max_block_size_,
        String alias_,
        bool bucket_scan_,
        PlanHints hints_,
        Assignments inline_expressions_,
        std::shared_ptr<AggregatingStep> aggregation_,
        std::shared_ptr<ProjectionStep> projection_,
        std::shared_ptr<FilterStep> filter_,
        DataStream table_output_stream_)
        : ISourceStep(std::move(output), hints_)
        , storage(storage_)
        , storage_id(storage_id_)
        , metadata_snapshot(metadata_snapshot_)
        , storage_snapshot(storage_snapshot_)
        , original_table(std::move(original_table_))
        , column_names(std::move(column_names_))
        , column_alias(std::move(column_alias_))
        , query_info(std::move(query_info_))
        , max_block_size(max_block_size_)
        , inline_expressions(std::move(inline_expressions_))
        , pushdown_aggregation(std::move(aggregation_))
        , pushdown_projection(std::move(projection_))
        , pushdown_filter(std::move(filter_))
        , table_output_stream(std::move(table_output_stream_))
        , bucket_scan(bucket_scan_)
        , alias(alias_)
        , log(getLogger("TableScanStep"))
    {
        if (storage)
            storage_id.uuid = storage->getStorageUUID();
    }

    String getName() const override { return "TableScan"; }
    Type getType() const override { return Type::TableScan; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;
    void toProto(Protos::TableScanStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<TableScanStep> fromProto(const Protos::TableScanStep & proto, ContextPtr context);

    const String & getDatabase() const { return storage_id.database_name; }
    const String & getTable() const { return storage_id.table_name; }
    const String & getTableAlias() const { return alias; }
    //    void setTable(const String & table_);
    void setOriginalTable(const String & original_table_) { original_table = original_table_; }
    const String & getOriginalTable() const { return original_table.empty() ? storage_id.table_name : original_table; }
    const Names & getColumnNames() const { return column_names; }
    const NamesWithAliases & getColumnAlias() const { return column_alias; }
    NameToNameMap getColumnToAliasMap() const;
    NameToNameMap getAliasToColumnMap() const;
    QueryProcessingStage::Enum getProcessedStage() const;
    size_t getMaxBlockSize() const;

    void setPushdownAggregation(QueryPlanStepPtr aggregation_)
    {
        pushdown_aggregation = std::dynamic_pointer_cast<AggregatingStep>(aggregation_);
    }
    void setPushdownProjection(QueryPlanStepPtr projection_)
    {
        pushdown_projection = std::dynamic_pointer_cast<ProjectionStep>(projection_);
    }
    void setPushdownFilter(QueryPlanStepPtr filter_) { pushdown_filter = std::dynamic_pointer_cast<FilterStep>(filter_); }
    std::shared_ptr<AggregatingStep> getPushdownAggregation() const { return pushdown_aggregation; }
    std::shared_ptr<ProjectionStep> getPushdownProjection() const { return pushdown_projection; }
    std::shared_ptr<FilterStep> getPushdownFilter() const { return pushdown_filter; }
    const AggregatingStep * getPushdownAggregationCast() const { return dynamic_cast<AggregatingStep *>(pushdown_aggregation.get()); }
    const ProjectionStep * getPushdownProjectionCast() const { return dynamic_cast<ProjectionStep *>(pushdown_projection.get()); }
    const FilterStep * getPushdownFilterCast() const { return dynamic_cast<FilterStep *>(pushdown_filter.get()); }
    AggregatingStep * getPushdownAggregationCast() { return dynamic_cast<AggregatingStep *>(pushdown_aggregation.get()); }
    ProjectionStep * getPushdownProjectionCast() { return dynamic_cast<ProjectionStep *>(pushdown_projection.get()); }
    FilterStep * getPushdownFilterCast() { return dynamic_cast<FilterStep *>(pushdown_filter.get()); }

    void setInlineExpressions(Assignments new_inline_expressions, ContextPtr context);
    const Assignments & getInlineExpressions() const
    {
        return inline_expressions;
    }
    bool hasInlineExpressions() const
    {
        return !inline_expressions.empty();
    }

    const DataStream & getTableOutputStream() const
    {
        return table_output_stream;
    }

    void setReadOrder(SortDescription read_order);
    SortDescription getReadOrder() const;

    void formatOutputStream(ContextPtr context);

    bool setLimit(size_t limit, const ContextMutablePtr & context);
    bool hasLimit() const;
    bool hasPrewhere() const;
    ASTPtr getPrewhere() const;

    SelectQueryInfo fillQueryInfo(ContextPtr context);
    void fillPrewhereInfo(ContextPtr context);
    void makeSetsForIndex(const ASTPtr & node, ContextPtr context, PreparedSets & prepared_sets) const;
    void fillQueryInfoV2(ContextPtr context);

    void allocate(ContextPtr context);
    Int32 getUniqueId() const { return unique_id; }
    void setUniqueId(Int32 unique_id_) { unique_id = unique_id_; }
    bool isBucketScan() const { return bucket_scan; }
    void setBucketScan(bool bucket_scan_) { bucket_scan = bucket_scan_; }
    // ues for plan cache
    void cleanStorage();
    void setStorage(ContextPtr context) { storage = DatabaseCatalog::instance().getTable(storage_id, context); }
    std::shared_ptr<IStorage> getStorage() const;
    const SelectQueryInfo & getQueryInfo() const { return query_info; }
    SelectQueryInfo & getQueryInfo()
    {
        return query_info;
    }
    const StorageID & getStorageID() const { return storage_id; }
    StorageMetadataPtr getMetadataSnapshot() const { return metadata_snapshot; }
    StorageSnapshotPtr getStorageSnapshot() const { return storage_snapshot; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr context) const override;

    enum GetFlags : UInt32
    {
        Output = 1,
        Prewhere = 2,
        BitmapIndex = 4,

        OutputAndPrewhere = Output | Prewhere,
        All = Output | Prewhere | BitmapIndex,
    };

    Names getRequiredColumns(GetFlags flags = All) const;

    void prepare(const PreparedStatementContext & prepared_context) override;

private:
    StoragePtr storage;
    StorageID storage_id;
    StorageMetadataPtr metadata_snapshot;
    StorageSnapshotPtr storage_snapshot;
    String original_table;
    Names column_names; // TODO: remove me, use column_alias instead
    NamesWithAliases column_alias;
    // Used for passing some important information to instruct data reading processing, including
    // - query.where(), condition of this storage, used for index pruning
    // - query.prewhere(), user specified PREWHERE of this storage. TODO: move move_where_to_prewhere optimize into PlanOptimizer
    // - partition_filter, condition on partition keys, used for partition pruning
    // - query.limit(), result limit of table scan
    SelectQueryInfo query_info;
    size_t max_block_size;

    // Expressions which can be calculated by IStorage::read/readFromParts, including
    // - bitmap index expressions
    // - sub expression of prewhere
    Assignments inline_expressions;

    // Pushdown steps. Now TableScanStep is not like a single step anymore, but more like a sub plan
    // with structure `Partial Aggregate->Projection->Filter->ReadTable`. And we are able to use
    // **clickhouse projection** to optimize its execution.
    // TODO: better to use a new kind of IQueryPlanStep
    std::shared_ptr<AggregatingStep> pushdown_aggregation;
    std::shared_ptr<ProjectionStep> pushdown_projection;
    std::shared_ptr<FilterStep> pushdown_filter;
    DataStream table_output_stream;

    // just for cascades, in order to distinguish between the same tables.
    Int32 unique_id{0};
    bool bucket_scan;
    String alias;

    // Only for worker.
    bool is_null_source{false};

    LoggerPtr log;

    // Optimises the where clauses for a bucket table by rewriting the IN clause and hence reducing the IN set size
    void rewriteInForBucketTable(ContextPtr context) const;
    void rewriteDynamicFilter(SelectQueryInfo & select_query, const BuildQueryPipelineSettings & build_settings, bool use_expand_pipe);

    void aliasColumns(QueryPipeline & pipeline, const BuildQueryPipelineSettings &, const String & pipeline_name);
    void setQuotaAndLimits(QueryPipeline & pipeline, const SelectQueryOptions & options, const BuildQueryPipelineSettings &);

    bool hasFunctionCanUseBitmapIndex() const;
    void initMetadataAndStorageSnapshot(ContextPtr context);
    Names getRequiredColumnsAndPartitionColumns() const;
};

}
