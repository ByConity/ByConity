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

#include <type_traits>
#include <QueryPlan/PlanVisitor.h>
#include <Optimizer/CostModel/PlanNodeCost.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/ProcessorProfile.h>

#include <Poco/JSON/Object.h>

namespace DB
{
using PlanCostMap = std::unordered_map<PlanNodeId, double>;
struct PlanSegmentDescription;
using PlanSegmentDescriptionPtr = std::shared_ptr<PlanSegmentDescription>;
using PlanSegmentDescriptions = std::vector<PlanSegmentDescriptionPtr>;

struct Analysis;
using AnalysisPtr = std::shared_ptr<Analysis>;

class PlanPrinter
{
public:
    PlanPrinter() = delete;

    static String textPlanNode(PlanNodePtr plan)
    {
        return textPlanNode(*plan);
    }
    static String textPlanNode(PlanNodeBase & node);
    static String textLogicalPlan(
        QueryPlan & plan,
        ContextMutablePtr context,
        PlanCostMap costs = {},
        const StepAggregatedOperatorProfiles & profiles = {},
        const QueryPlanSettings & settings = {});
    static String jsonLogicalPlan(
        QueryPlan & plan,
        std::optional<PlanNodeCost> plan_cost,
        const StepAggregatedOperatorProfiles & profiles = {},
        const PlanCostMap & costs = {},
        const QueryPlanSettings & settings = {});
    static String jsonDistributedPlan(PlanSegmentDescriptions & segment_descs, const StepAggregatedOperatorProfiles & profiles);
    static String textDistributedPlan(
        PlanSegmentDescriptions & segments_desc,
        ContextMutablePtr context,
        const std::unordered_map<PlanNodeId, double> & costs = {},
        const StepAggregatedOperatorProfiles & profiles = {},
        const QueryPlan & query_plan = {},
        const QueryPlanSettings & settings = {});
    static String textPipelineProfile(PlanSegmentDescriptions & segment_descs, SegmentAndWorkerToGroupedProfile & worker_grouped_profiles);
    static String jsonPipelineProfile(PlanSegmentDescriptions & segment_descs, SegmentAndWorkerToGroupedProfile & worker_grouped_profiles);
    static void getPlanNodes(const PlanNodePtr & parent, std::unordered_map<PlanNodeId, PlanNodePtr> & id_to_node);
    static std::unordered_map<PlanNodeId, PlanNodePtr>  getPlanNodeMap(const QueryPlan & query_plan);
    static void getRemoteSegmentId(const QueryPlan::Node * node, std::unordered_map<PlanNodeId, size_t> & exchange_to_segment);
    static String getPlanSegmentHeaderText(PlanSegmentDescriptionPtr & segment_desc);

    static String jsonMetaData(
        ASTPtr & query, AnalysisPtr analysis, ContextMutablePtr context, QueryPlanPtr & plan, const QueryMetadataSettings & settings = {});

    class TextPrinter;
};

class TextPrinterIntent
{
public:
    static constexpr auto VERTICAL_LINE = "│  ";
    static constexpr auto INTERMEDIATE_PREFIX = "├─ ";
    static constexpr auto LAST_PREFIX = "└─ ";
    static constexpr auto EMPTY_PREFIX = "   ";

    TextPrinterIntent() = default;
    explicit TextPrinterIntent(size_t prefix, bool has_children_)
        : current_lines_prefix(std::string(prefix, ' '))
        , next_lines_prefix(std::string(prefix, ' '))
        , hasChildren(has_children_)
    {
    }

    TextPrinterIntent forChild(bool last, bool has_children_) const;
    String print() const { return current_lines_prefix; }
    String detailIntent() const;

private:
    TextPrinterIntent(String current_lines_prefix_, String next_lines_prefix_, bool hasChildren);

    String current_lines_prefix;
    String next_lines_prefix;
    bool hasChildren{true};
};

class PlanPrinter::TextPrinter
{
public:
    explicit TextPrinter(
        const std::unordered_map<PlanNodeId, double> & costs_,
        ContextMutablePtr context_ = nullptr,
        bool is_distributed_ = false,
        const std::unordered_map<PlanNodeId, size_t> & exchange_to_segment_ = {},
        QueryPlanSettings settings_ = {},
        size_t max_predicate_text_length_ = 10000)
        : costs(costs_)
        , is_distributed(is_distributed_)
        , exchange_to_segment(exchange_to_segment_)
        , context(context_)
        , settings(settings_)
        , max_predicate_text_length(max_predicate_text_length_)
    {}
    static String printOutputColumns(PlanNodeBase & plan_node, const TextPrinterIntent & intent = {});
    String printLogicalPlan(PlanNodeBase & plan, const TextPrinterIntent & intent = {}, const StepAggregatedOperatorProfiles & profiles = {});
    String printPipelineProfile(GroupedProcessorProfilePtr & input_root, const TextPrinterIntent & intent = {});

    static String prettyNum(size_t num);
    static String prettyBytes(size_t bytes);
    static String prettySeconds(size_t seconds);
    static String printPrefix(PlanNodeBase & plan);
    String printSuffix(PlanNodeBase & plan);
    static String printQError(const PlanNodeBase & plan, const StepAggregatedOperatorProfiles & profiles);
    static String printFilter(ConstASTPtr filter, size_t max_text_length = 10000);
private:
    String printDetail(QueryPlanStepPtr plan, const TextPrinterIntent & intent) const;
    static String printProcessorDetail(GroupedProcessorProfilePtr profile, const TextPrinterIntent & intent);
    String printStatistics(const PlanNodeBase & plan, const TextPrinterIntent & intent = {}) const;
    static String printOperatorProfiles(PlanNodeBase & plan, const TextPrinterIntent & intent = {}, const StepAggregatedOperatorProfiles & profiles = {}) ;

    const std::unordered_map<PlanNodeId, double> & costs;
    bool is_distributed;
    const std::unordered_map<PlanNodeId, size_t> & exchange_to_segment;
    ContextMutablePtr context;
    QueryPlanSettings settings;
    const size_t max_predicate_text_length;
};

class NodeDescription;
using NodeDescriptionPtr = std::shared_ptr<NodeDescription>;
using NodeDescriptions = std::vector<NodeDescriptionPtr>;

class NodeDescription
{
public:
    size_t node_id;
    IQueryPlanStep::Type type = IQueryPlanStep::Type::Any;
    String step_name;
    std::unordered_map<String, String> step_detail;
    std::unordered_map<String, std::vector<String>> step_vector_detail;
    std::unordered_map<String, NodeDescriptionPtr> descriptions_in_step;
    std::vector<NodeDescriptionPtr> children;

    struct StatisticInfo
    {
        size_t row_count = 0;
    };

    std::optional<StatisticInfo> stats;

    void setStepStatistic(PlanNodePtr node);
    void setStepDetail(QueryPlanStepPtr step);
    Poco::JSON::Object::Ptr jsonNodeDescription(const StepAggregatedOperatorProfiles & profiles, bool print_stats, const PlanCostMap & costs = {});
    static NodeDescriptionPtr getPlanDescription(QueryPlan::Node * node);
    static NodeDescriptionPtr getPlanDescription(PlanNodePtr node);
};

struct PlanSegmentDescription
{
    struct OutputInfo
    {
        size_t segment_id;
        String plan_segment_type;
        ExchangeMode mode;
        size_t exchange_id;
        size_t parallel_size;
        bool keep_order;
    };
    struct InputInfo
    {
        size_t segment_id;
        ExchangeMode mode;
        size_t exchange_id;
        size_t exchange_parallel_size;
        bool keep_order;
        bool stable;
    };
    size_t segment_id;
    String segment_type;
    String query_id;

    PlanNodeId root_id;
    PlanNodeId root_child_id;
    PlanNodePtr plan_node = nullptr;

    String cluster_name;
    size_t parallel;
    size_t exchange_parallel_size;
    UInt32 shard_num;
    ExchangeMode mode;
    Names shuffle_keys;
    std::unordered_map<PlanNodeId, size_t> exchange_to_segment;
    std::vector<std::shared_ptr<OutputInfo>> outputs_desc;
    std::vector<std::shared_ptr<InputInfo>> inputs_desc;


    std::vector<String> output_columns;

    NodeDescriptionPtr node_description;

    Poco::JSON::Object::Ptr jsonPlanSegmentDescription(const StepAggregatedOperatorProfiles & profiles, bool is_pipeline = false);
    String jsonPlanSegmentDescriptionAsString(const StepAggregatedOperatorProfiles & profiles);
    static PlanSegmentDescriptionPtr getPlanSegmentDescription(PlanSegmentPtr & segment, bool record_plan_detail = false);
};

}
