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
#include <utility>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/prepared_statement.h>
#include <Parsers/IAST_fwd.h>
#include <Protos/EnumMacros.h>
#include <Protos/plan_node.pb.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/Hints/IPlanHint.h>
#include <QueryPlan/PlanSerDerHelper.h>


namespace JSONBuilder
{
class JSONMap;
}

namespace DB
{

namespace Protos
{
    class DataStream;
}

class QueryPipeline;
using QueryPipelinePtr = std::unique_ptr<QueryPipeline>;
using QueryPipelines = std::vector<QueryPipelinePtr>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::vector<ProcessorPtr>;

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

namespace JSONBuilder
{
    class JSONMap;
}

/// Description of data stream.
/// Single logical data stream may relate to many ports of pipeline.
class DataStream
{
public:
    Block header;

    /// Tuples with those columns are distinct.
    /// It doesn't mean that columns are distinct separately.
    /// Removing any column from this list brakes this invariant.
    NameSet distinct_columns = {};

    /// QueryPipeline has single port. Totals or extremes ports are not counted.
    bool has_single_port = false;

    /// How data is sorted.
    ENUM_WITH_PROTO_CONVERTER(
        SortMode, // enum name
        Protos::DataStream::SortMode, // proto enum message
        (Chunk, 0), /// Separate chunks are sorted
        (Port, 1), /// Data from each port is sorted
        (Stream, 2) /// Data is globally sorted
    );

    /// It is not guaranteed that header has columns from sort_description.
    SortDescription sort_description = {};
    SortMode sort_mode = SortMode::Chunk;

    /// Things which may be added:
    /// * limit
    /// * estimated rows number
    /// * memory allocation context

    bool hasEqualPropertiesWith(const DataStream & other) const
    {
        return distinct_columns == other.distinct_columns && has_single_port == other.has_single_port
            && sort_description == other.sort_description && (sort_description.empty() || sort_mode == other.sort_mode);
    }

    bool hasEqualHeaderWith(const DataStream & other) const { return blocksHaveEqualStructure(header, other.header); }

    NamesAndTypes getNamesAndTypes() const { return header.getNamesAndTypes(); }

    NameToType getNamesToTypes() const { return header.getNamesToTypes(); }

    Names getNames() const
    {
        return header.getNames();
    }

    void toProto(Protos::DataStream & proto) const;
    void fillFromProto(const Protos::DataStream & proto);
};

using DataStreams = std::vector<DataStream>;

class IQueryPlanStep;
using QueryPlanStepPtr = std::shared_ptr<IQueryPlanStep>;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

using PlanHints = std::vector<PlanHintPtr>;

struct RuntimeAttributeDescription
{
    String description;
    std::vector<std::pair<String, String>> name_and_detail;

    // If the attribute information is complex, can use json
    String additional;

    void fillFromProto(const Protos::RuntimeAttributeDescription & proto);
    void toProto(Protos::RuntimeAttributeDescription & proto) const;
};


/// Single step of query plan.
class IQueryPlanStep
{
public:
// this must match protobuf message QueryPlanStep
// in src/Protos/plan_node.proto
// where Step/_step postfix is attached.

// Use for Optimizer
#define APPLY_STEP_PROTOBUF_TYPES_AND_NAMES(MM) \
    MM(Aggregating, aggregating) \
    MM(ArrayJoin, array_join) \
    MM(AssignUniqueId, assign_unique_id) \
    MM(CTERef, c_t_e_ref) \
    MM(Distinct, distinct) \
    MM(EnforceSingleRow, enforce_single_row) \
    MM(Except, except) \
    MM(Exchange, exchange) \
    MM(Extremes, extremes) \
    MM(Filling, filling) \
    MM(Filter, filter) \
    MM(Intersect, intersect) \
    MM(Join, join) \
    MM(LimitBy, limit_by) \
    MM(Limit, limit) \
    MM(MarkDistinct, mark_distinct) \
    MM(MergeSorting, merge_sorting) \
    MM(MergingAggregated, merging_aggregated) \
    MM(MergingSorted, merging_sorted) \
    MM(PartialSorting, partial_sorting) \
    MM(PartitionTopN, partition_top_n) \
    MM(Projection, projection) \
    MM(Expand, expand) \
    MM(ReadNothing, read_nothing) \
    MM(ReadStorageRowCount, read_storage_row_count) \
    MM(RemoteExchangeSource, remote_exchange_source) \
    MM(Sorting, sorting) \
    MM(TableFinish, table_finish) \
    MM(TableScan, table_scan) \
    MM(TableWrite, table_write) \
    MM(TopNFiltering, top_n_filtering) \
    MM(Union, union) \
    MM(Window, window) \
    MM(Values, values) \
    MM(IntersectOrExcept, intersect_or_except) \
    MM(Buffer, buffer)\
    MM(Apply, apply) \
    MM(ExplainAnalyze, explain_analyze) \
    MM(FinalSample, final_sample) \
    MM(Offset, offset) \
    MM(FinishSorting, finish_sorting) \
    MM(TotalsHaving, totals_having) \
    MM(OutfileWrite, outfile_write) \
    MM(OutfileFinish, outfile_finish) \
    MM(LocalExchange, local_exchange) \
    MM(IntermediateResultCache, intermediate_result_cache) \
    MM(MultiJoin, multi_join)

// macro helpers to convert MM(x, y) to M(x)
#define IMPL_TUPLE_TO_FIRST(_x, _y) (_x)
#define IMPL_STEPS_PROTOBUF_TYPES APPLY_STEP_PROTOBUF_TYPES_AND_NAMES(IMPL_TUPLE_TO_FIRST)
#define IMPL_MACRO_FUNCTION_APPLY(_r, _data, _elem) _data(_elem)

// apply unary macro
// M(Apply) M(Join) M(Aggregating)...
#define APPLY_STEP_TYPES(M) BOOST_PP_SEQ_FOR_EACH(IMPL_MACRO_FUNCTION_APPLY, M, IMPL_STEPS_PROTOBUF_TYPES)


#define ENUM_DEF(ITEM) ITEM,

    enum class Type
    {
        Any = 0,
        // change this when order is changed to avoid conflicts
        StepBegin = 100,
        APPLY_STEP_TYPES(ENUM_DEF) UNDEFINED,
        ReadFromMergeTree,
        ReadFromCnchHive,
        ReadFromCnchFile,
        ReadFromPreparedSource,
        NullSource,
        Tree,
        CreatingSet,
        CreatingSets,
        Cube,
        Expression,
        FilledJoin,
        PlanSegmentSource,
        SettingQuotaAndLimits,
        ReadFromStorage,
        Rollup
    };

#undef ENUM_DEF

    IQueryPlanStep() = default;
    IQueryPlanStep(const IQueryPlanStep &) = default;
    IQueryPlanStep & operator=(IQueryPlanStep &) = default;
    virtual ~IQueryPlanStep() = default;

    virtual String getName() const = 0;

    virtual Type getType() const = 0;

    /// Add processors from current step to QueryPipeline.
    /// Calling this method, we assume and don't check that:
    ///   * pipelines.size() == getInputStreams.size()
    ///   * header from each pipeline is the same as header from corresponding input_streams
    /// Result pipeline must contain any number of streams with compatible output header is hasOutputStream(),
    ///   or pipeline should be completed otherwise.
    virtual QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & settings) = 0;
    static ActionsDAGPtr createFilterExpressionActions(ContextPtr context, const ASTPtr & filter, const Block & header);
    static ActionsDAGPtr createExpressionActions(
        ContextPtr context, const NamesAndTypesList & source, const Names & output, const ASTPtr & ast, bool add_project = true);
    static ActionsDAGPtr createExpressionActions(
        ContextPtr context, const NamesAndTypesList & source, const NamesWithAliases & output, const ASTPtr & ast, bool add_project = true);
    static void projection(QueryPipeline & pipeline, const Block & target, const BuildQueryPipelineSettings & settings);
    static void aliases(QueryPipeline & pipeline, const Block & target, const BuildQueryPipelineSettings & settings);

    const DataStreams & getInputStreams() const { return input_streams; }
    virtual void setInputStreams(const DataStreams & input_streams_) = 0;

    void addHints(SqlHints & sql_hints, ContextMutablePtr & context, bool check_type = false);

    const PlanHints & getHints() const
    {
        return hints;
    }

    void setHints(const PlanHints & new_hints)
    {
        hints = new_hints;
    }


    bool hasOutputStream() const { return output_stream.has_value(); }
    const DataStream & getOutputStream() const;

    /// Methods to describe what this step is needed for.
    const std::string & getStepDescription() const { return step_description; }
    void setStepDescription(std::string description) { step_description = std::move(description); }

    struct FormatSettings
    {
        WriteBuffer & out;
        size_t offset = 0;
        const size_t indent = 2;
        const char indent_char = ' ';
        const bool write_header = false;
    };

    /// Get detailed description of step actions. This is shown in EXPLAIN query with options `actions = 1`.
    virtual void describeActions(JSONBuilder::JSONMap & /*map*/) const
    {
    }
    virtual void describeActions(FormatSettings & /*settings*/) const
    {
    }

    /// Get detailed description of read-from-storage step indexes (if any). Shown in with options `indexes = 1`.
    virtual void describeIndexes(JSONBuilder::JSONMap & /*map*/) const
    {
    }
    virtual void describeIndexes(FormatSettings & /*settings*/) const
    {
    }

    /// Get description of processors added in current step. Should be called after updatePipeline().
    virtual void describePipeline(FormatSettings & /*settings*/) const
    {
    }

    virtual bool isPhysical() const { return true; }
    virtual bool isLogical() const { return true; }

    virtual std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const = 0;

    size_t hash(bool ignore_output_stream = true) const;

    bool operator==(const IQueryPlanStep & r) const
    {
        return isPlanStepEqual(*this, r);
    }
    static String toString(Type type);

    virtual void prepare(const PreparedStatementContext &)
    {
    }

    std::unordered_map<String, RuntimeAttributeDescription> & getAttributeDescriptions()
    {
        return attribute_descriptions;
    }

protected:
    DataStreams input_streams;
    std::optional<DataStream> output_stream;

    /// Text description about what current step does.
    std::string step_description;
    PlanHints hints;

    /// Text description of runtime attributes
    std::unordered_map<String, RuntimeAttributeDescription> attribute_descriptions;

    static void describePipeline(const Processors & processors, FormatSettings & settings);
};

namespace Protos
{
#define CASE_DEF(TYPE) class TYPE##Step;
    APPLY_STEP_TYPES(CASE_DEF)
#undef CASE_DEF
}
}
