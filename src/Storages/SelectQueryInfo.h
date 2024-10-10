/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/PreparedSets.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/IntermediateResult/TableScanCacheInfo.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/Index/MergeTreeIndexHelper.h>
#include <sstream>
#include <Storages/ProjectionsDescription.h>
#include <memory>
#include <vector>

namespace DB
{

namespace Protos
{
    class InputOrderInfo;
    class SelectQueryInfo;
}

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

struct AtomicPredicate;
using AtomicPredicatePtr = std::shared_ptr<AtomicPredicate>;

struct FilterInfo;
using FilterInfoPtr = std::shared_ptr<FilterInfo>;

struct FilterDAGInfo;
using FilterDAGInfoPtr = std::shared_ptr<FilterDAGInfo>;

struct InputOrderInfo;
using InputOrderInfoPtr = std::shared_ptr<InputOrderInfo>;

struct TreeRewriterResult;
using TreeRewriterResultPtr = std::shared_ptr<const TreeRewriterResult>;

class ReadInOrderOptimizer;
using ReadInOrderOptimizerPtr = std::shared_ptr<const ReadInOrderOptimizer>;

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

struct MergeTreeDataSelectAnalysisResult;
using MergeTreeDataSelectAnalysisResultPtr = std::shared_ptr<MergeTreeDataSelectAnalysisResult>;

class ReadFromMergeTree;
//struct ReadFromMergeTree::IndexStat;

struct PrewhereInfo
{
    /// Actions which are executed in order to alias columns are used for prewhere actions.
    ActionsDAGPtr alias_actions;
    /// Actions for row level security filter. Applied separately before prewhere_actions.
    /// This actions are separate because prewhere condition should not be executed over filtered rows.
    ActionsDAGPtr row_level_filter;
    /// Actions which are executed on block in order to get filter column for prewhere step.
    ActionsDAGPtr prewhere_actions;
    String row_level_column_name;
    String prewhere_column_name;
    bool remove_prewhere_column = false;
    bool need_filter = false;
    MergeTreeIndexContextPtr index_context;

    PrewhereInfo() = default;
    PrewhereInfo(const PrewhereInfo &) = default;
    explicit PrewhereInfo(ActionsDAGPtr prewhere_actions_, String prewhere_column_name_)
            : prewhere_actions(std::move(prewhere_actions_)), prewhere_column_name(std::move(prewhere_column_name_)) {}

    std::string dump() const;
};

/// An atomic predicate expression associated with a group of columns
struct AtomicPredicate
{
    ActionsDAGPtr predicate_actions;
    /// Name of the filter column after executing the predicate action
    String filter_column_name;
    /// Is this predicate a row level filter? If yes, then we MUST filter the block with this fitlter first
    /// before execute any other predicates to prevent predicate injection attack. Currently, row-level filter
    /// comes from 2 sources: (1) delete bitmap for unique table and (2) row-level policy conditions.
    bool is_row_filter = false;
    /// Is this predicate is actually a bitmap index? If yes, then we don't read the actual column but read the
    /// index instead.
    MergeTreeIndexContextPtr index_context;
    /// Should we keep the filter column or not, unused for now
    [[maybe_unused]] bool remove_filter_column = false;
    AtomicPredicate() = default;
    explicit AtomicPredicate(ActionsDAGPtr prewhere_actions_, String prewhere_column_name_)
            : predicate_actions(std::move(prewhere_actions_)), filter_column_name(std::move(prewhere_column_name_)) {}
    String dump() const;
};

/// Helper struct to store all the information about the filter expression.
struct FilterInfo
{
    ExpressionActionsPtr alias_actions;
    ExpressionActionsPtr actions;
    String column_name;
    bool do_remove_column = false;
};

/// Same as FilterInfo, but with ActionsDAG.
struct FilterDAGInfo
{
    ActionsDAGPtr actions;
    String column_name;
    bool do_remove_column = false;

    std::string dump() const;
};

struct InputOrderInfo
{
    SortDescription order_key_prefix_descr;
    int direction;

    InputOrderInfo(const SortDescription & order_key_prefix_descr_, int direction_)
        : order_key_prefix_descr(order_key_prefix_descr_), direction(direction_) {}

    InputOrderInfo() = default;

    bool operator ==(const InputOrderInfo & other) const
    {
        return order_key_prefix_descr == other.order_key_prefix_descr && direction == other.direction;
    }

    bool operator !=(const InputOrderInfo & other) const { return !(*this == other); }

    void toProto(Protos::InputOrderInfo & proto) const;
    static std::shared_ptr<InputOrderInfo> fromProto(const Protos::InputOrderInfo & proto);
};

class IMergeTreeDataPart;

using ManyExpressionActions = std::vector<ExpressionActionsPtr>;

// The projection selected to execute current query
struct ProjectionCandidate
{
    const ProjectionDescription * desc{};
    PrewhereInfoPtr prewhere_info;
    ActionsDAGPtr before_where;
    String where_column_name;
    bool remove_where_filter = false;
    ActionsDAGPtr before_aggregation;
    Names required_columns;
    NamesAndTypesList aggregation_keys;
    AggregateDescriptions aggregate_descriptions;
    bool aggregate_overflow_row = false;
    bool aggregate_final = false;
    bool complete = false;
    ReadInOrderOptimizerPtr order_optimizer;
    InputOrderInfoPtr input_order_info;
    ManyExpressionActions group_by_elements_actions;
};

class InterpreterSelectQuery;
/** Query along with some additional data,
  *  that can be used during query processing
  *  inside storage engines.
  */
struct SelectQueryInfo
{
    ASTPtr query;
    ASTPtr view_query; /// Optimized VIEW query
    ASTPtr partition_filter; /// partition filter

    /// Cluster for the query.
    ClusterPtr cluster;
    /// Optimized cluster for the query.
    /// In case of optimize_skip_unused_shards it may differs from original cluster.
    ///
    /// Configured in StorageDistributed::getQueryProcessingStage()
    ClusterPtr optimized_cluster;

    TreeRewriterResultPtr syntax_analyzer_result;

    PrewhereInfoPtr prewhere_info;

    ReadInOrderOptimizerPtr order_optimizer;
    /// Can be modified while reading from storage
    InputOrderInfoPtr input_order_info;

    MergeTreeIndexContextPtr index_context;

    /// Prepared sets are used for indices by storage engine.
    /// Example: x IN (1, 2, 3)
    PreparedSets sets;

    /// Cached value of ExpressionAnalysisResult::has_window
    bool has_window = false;

    ClusterPtr getCluster() const { return !optimized_cluster ? cluster : optimized_cluster; }

    /// If not null, it means we choose a projection to execute current query.
    std::optional<ProjectionCandidate> projection;
    bool ignore_projections = false;
    bool is_projection_query = false;

    /// Read from local table
    bool read_local_table = true;

    bool optimize_trivial_count = false;

    /// predicate ast
    std::vector<ASTPtr> atomic_predicates_expr;
    /// atomic predicate, may > predicate ast
    std::deque<AtomicPredicatePtr> atomic_predicates;

    /// cache digest, used for matching with cache when enable query cache
    TableScanCacheInfo cache_info;

    void serialize(WriteBuffer &) const;
    void deserialize(ReadBuffer &);
    /// Read from index
    bool read_bitmap_index = false;

    void toProto(Protos::SelectQueryInfo & proto) const;
    void fillFromProto(const Protos::SelectQueryInfo & proto);

    const ASTSelectQuery * getSelectQuery() const
    {
        if (!query)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query info query is not set");

        auto * select_query = query->as<ASTSelectQuery>();
        if (!select_query)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query info query is not a ASTSelectQuery");

        return select_query;
    }

    void appendPartitonFilters(ASTs conjuncts);

    ASTSelectQuery * getSelectQuery()
    {
        return const_cast<ASTSelectQuery *>((const_cast<const SelectQueryInfo *>(this))->getSelectQuery());
    }
};

/// Collect all query 's predicates from query info, mainly used for collecting index.
///  The predicates may come from 3 sources:
/// - query.where()
/// - query.prewhere()
/// - atomic_predicates_expr
ASTPtr getFilterFromQueryInfo(const SelectQueryInfo & query_info, bool clone = false);

/// Helper function for dealing with projection
const PrewhereInfoPtr & getPrewhereInfo(const SelectQueryInfo & query_info);

const std::deque<AtomicPredicatePtr> & getAtomicPredicates(const SelectQueryInfo & query_info);

MergeTreeIndexContextPtr getIndexContext(const SelectQueryInfo & query_info);

TableScanCacheInfo getTableScanCacheInfo(const SelectQueryInfo & query_info);

ASTPtr rewriteSampleForDistributedTable(const ASTPtr & query_ast, size_t shard_size);

}
