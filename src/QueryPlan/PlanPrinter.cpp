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

#include <QueryPlan/PlanPrinter.h>

#include <Analyzers/ASTEquals.h>
#include <Optimizer/PlanNodeSearcher.h>
#include <Optimizer/PredicateConst.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Utils.h>
#include <Optimizer/OptimizerMetrics.h>
#include <AggregateFunctions/AggregateFunctionNull.h>
#include <Poco/JSON/Object.h>
#include <Parsers/formatAST.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <Interpreters/convertFieldToType.h>

#include <utility>

namespace DB
{
namespace
{
template <class V>
String join(const V & v, const String & sep, const String & prefix = {}, const String & suffix = {})
{
    std::stringstream out;
    out << prefix;
    if (!v.empty())
    {
        auto it = v.begin();
        out << *it;
        for (++it; it != v.end(); ++it)
            out << sep << *it;
    }
    out << suffix;
    return out.str();
}
}

String PlanPrinter::textPlanNode(PlanNodeBase & node)
{
    PlanCostMap costs;
    StepAggregatedOperatorProfiles profiles;
    TextPrinter printer{true, true, costs};
    bool has_children = node.getChildren().empty();
    return printer.printLogicalPlan(node, TextPrinterIntent{0, has_children}, profiles);
}

String PlanPrinter::textLogicalPlan(
    QueryPlan & plan,
    ContextMutablePtr context,
    bool print_stats,
    bool verbose,
    PlanCostMap costs,
    const StepAggregatedOperatorProfiles & profiles,
    bool print_profile)
{
    TextPrinter printer{print_stats, verbose, costs, false, {}, print_profile};
    bool has_children = !plan.getPlanNode()->getChildren().empty();
    auto output = printer.printLogicalPlan(*plan.getPlanNode(), TextPrinterIntent{0, has_children}, profiles);

    for (auto & item : plan.getCTEInfo().getCTEs())
    {
        output += "CTEDef [" + std::to_string(item.first) + "]\n";
        output += printer.printLogicalPlan(*item.second, TextPrinterIntent{3, !item.second->getChildren().empty()}, profiles);
    }

    auto magic_sets = PlanNodeSearcher::searchFrom(plan)
                          .where([](auto & node) {
                              return node.getStep()->getType() == IQueryPlanStep::Type::Join
                                  && dynamic_cast<const JoinStep &>(*node.getStep()).isMagic();
                          })
                          .count();

    if (magic_sets > 0)
        output += "note: Magic Set is applied for " + std::to_string(magic_sets) + " parts.\n";

    auto filter_nodes = PlanNodeSearcher::searchFrom(plan)
                            .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Filter; })
                            .findAll();

    size_t runtime_filters = 0;
    for (auto & filter : filter_nodes)
    {
        const auto * filter_step = dynamic_cast<const FilterStep *>(filter->getStep().get());
        auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter_step->getFilter());
        runtime_filters += filters.first.size();
    }

    if (runtime_filters > 0)
        output += "note: Runtime Filter is applied for " + std::to_string(runtime_filters) + " times.\n";

    auto cte_nodes = PlanNodeSearcher::searchFrom(plan)
                         .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::CTERef; })
                         .count();

    if (cte_nodes > 0)
        output += "note: CTE(Common Table Expression) is applied for " + std::to_string(cte_nodes) + " times.\n";

    auto & optimizer_metrics = context->getOptimizerMetrics();
    if (optimizer_metrics && !optimizer_metrics->getUsedMaterializedViews().empty())
    {
        output += "note: Materialized Views is applied for " + std::to_string(optimizer_metrics->getUsedMaterializedViews().size())
            + " times: ";
        const auto & views = optimizer_metrics->getUsedMaterializedViews();
        auto it = views.begin();
        output += it->getDatabaseName() + "." + it->getTableName();
        for (++it; it != views.end(); ++it)
            output += ", " + it->getDatabaseName() + "." + it->getTableName();
        output += ".";
    }

    return output;
}

String PlanPrinter::jsonLogicalPlan(QueryPlan & plan, bool print_stats, bool, std::optional<PlanNodeCost> plan_cost, const StepAggregatedOperatorProfiles & profiles)
{
    std::ostringstream os;
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(true);
    auto plannode_desc = NodeDescription::getPlanDescription(plan.getPlanNode());

    if (plan_cost.has_value())
    {
        auto cost = plan_cost.value();
        json->set("total_cost", cost.getCost());
        json->set("cpu_cost_value", cost.getCpuValue());
        json->set("net_cost_value", cost.getNetValue());
        json->set("men_cost_value", cost.getMenValue());
    }

    json->set("plan", plannode_desc->jsonNodeDescription(profiles, print_stats));
    if (!plan.getCTEInfo().getCTEs().empty())
    {
        Poco::JSON::Array ctes;
        for (auto & item : plan.getCTEInfo().getCTEs())
        {
            auto cte_desc = NodeDescription::getPlanDescription(item.second);
            ctes.add(cte_desc->jsonNodeDescription(profiles, print_stats));
        }
        json->set("CTEs", ctes);
    }

    json->stringify(os, 2);
    return os.str();
}

String PlanPrinter::getPlanSegmentHeaderText(PlanSegmentDescriptionPtr & segment_desc)
{
    auto f = [](ExchangeMode mode) {
        switch (mode)
        {
            case ExchangeMode::LOCAL_NO_NEED_REPARTITION:
                return "LOCAL_NO_NEED_REPARTITION";
            case ExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                return "LOCAL_MAY_NEED_REPARTITION";
            case ExchangeMode::BROADCAST:
                return "BROADCAST";
            case ExchangeMode::REPARTITION:
                return "REPARTITION";
            case ExchangeMode::GATHER:
                return "GATHER";
            default:
                return "UNKNOWN";
        }
    };

    std::ostringstream os;

    size_t segment_id = segment_desc->segment_id;
    os << "Segment[" << segment_id << "] [" + segment_desc->segment_type + "]\n";

    ExchangeMode mode = segment_desc->mode;
    String exchange = (segment_id == 0) ? "Output" : f(mode);
    os << "   Output Exchange: " << exchange;
    if (exchange == "REPARTITION") // print shuffle keys
    {
        os << "[";
        bool first = true;
        for (auto & key : segment_desc->shuffle_keys)
        {
            if (first)
            {
                os << key;
            }
            os << ", " << key;
        }
        os << "]";
    }
    os << "\n";

    os << "   Parallel Size: " << segment_desc->parallel;
    os << ", Cluster Name: " << (segment_desc->cluster_name.empty() ? "server" : segment_desc->cluster_name);
    os << ", Exchange Parallel Size: " << segment_desc->exchange_parallel_size  << "\n";
    if (!segment_desc->outputs_desc.empty())
    {
        os << "   Outputs: [";
        bool first = true;
        for (auto & output : segment_desc->outputs_desc)
        {
            if (!first)
                os << "\n             ";
            os << "( SegmentID:" << output->segment_id << " PlanSegmentType:" << output->plan_segment_type
                << " ParallelSize:" << output->parallel_size << ")";
            first = false;
        }
        os << "]\n";
    }
    return os.str();
}

String PlanPrinter::textDistributedPlan(
    PlanSegmentDescriptions & segments_desc,
    bool print_stats,
    bool verbose,
    const std::unordered_map<PlanNodeId, double> & costs,
    const StepAggregatedOperatorProfiles & profiles,
    const QueryPlan & query_plan,
    bool print_profile
)
{
    auto id_to_node = getPlanNodeMap(query_plan);
    for (auto & segment_desc : segments_desc)
    {
        if (segment_desc->segment_id == 0)
        {
            segment_desc->plan_node = query_plan.getPlanNodeRoot();
            continue;
        }

        if (segment_desc->root_id == 0)
            continue;

        PlanNodePtr plan_node;
        if (id_to_node.contains(segment_desc->root_id))
            plan_node = id_to_node.at(segment_desc->root_id);
        else if (segment_desc->root_child_id != 0)
            plan_node = id_to_node.at(segment_desc->root_child_id);
        else
            continue;

        segment_desc->plan_node = plan_node;
    }

    std::ostringstream os;

    auto cmp = [](const PlanSegmentDescriptionPtr & s1, const PlanSegmentDescriptionPtr & s2) { return s1->segment_id < s2->segment_id; };
    std::sort(segments_desc.begin(), segments_desc.end(), cmp);

    for (auto & segment_ptr : segments_desc)
    {
        os << getPlanSegmentHeaderText(segment_ptr);
        if (!segment_ptr->plan_node)
            continue;

        auto analyze_node = PlanNodeSearcher::searchFrom(segment_ptr->plan_node)
                                .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::ExplainAnalyze; })
                                .findFirst();
        if (analyze_node)
        {
            os << TextPrinter::printOutputColumns(*analyze_node.value()->getChildren()[0], TextPrinterIntent{3, false});
            TextPrinter printer{print_stats, verbose, costs, true, segment_ptr->exchange_to_segment, print_profile};
            bool has_children = !analyze_node.value()->getChildren().empty();
            if ((analyze_node.value()->getStep()->getType() == IQueryPlanStep::Type::CTERef || analyze_node.value()->getStep()->getType() == IQueryPlanStep::Type::Exchange))
                has_children = false;

            auto output = printer.printLogicalPlan(*analyze_node.value(), TextPrinterIntent{6, has_children}, profiles);
            os << output;
        }
        else
        {
            auto plan_root = segment_ptr->plan_node;
            os << TextPrinter::printOutputColumns(*segment_ptr->plan_node, TextPrinterIntent{3, false});
            TextPrinter printer{print_stats, verbose, costs, true, segment_ptr->exchange_to_segment, print_profile};
            bool has_children = !plan_root->getChildren().empty();
            if ((plan_root->getStep()->getType() == IQueryPlanStep::Type::CTERef || plan_root->getStep()->getType() == IQueryPlanStep::Type::Exchange))
                has_children = false;

            auto output = printer.printLogicalPlan(*segment_ptr->plan_node, TextPrinterIntent{6, has_children}, profiles);
            os << output;
        }

        os << "\n";
    }

    return os.str();
}


String PlanPrinter::textPipelineProfile(PlanSegmentDescriptions & segment_descs, SegmentAndWorkerToGroupedProfile & worker_grouped_profiles)
{
    std::ostringstream os;

    auto cmp = [](const PlanSegmentDescriptionPtr & s1, const PlanSegmentDescriptionPtr & s2) { return s1->segment_id < s2->segment_id; };
    std::sort(segment_descs.begin(), segment_descs.end(), cmp);

    for (auto & segment_ptr : segment_descs)
    {
        size_t segment_id = segment_ptr->segment_id;
        os << getPlanSegmentHeaderText(segment_ptr);
        if (!worker_grouped_profiles.contains(segment_id) || worker_grouped_profiles.at(segment_id).empty())
            continue;

        for (auto & [address, segment_profile] : worker_grouped_profiles.at(segment_id))
        {
            if (!segment_profile)
                continue;
            TextPrinterIntent print{3, false};
            os << print.print() << address << "\n";
            TextPrinter printer{false, false, {}};
            bool has_children = !segment_profile->children.empty();
            auto output = printer.printPipelineProfile(segment_profile, TextPrinterIntent{3, has_children});
            os << output;
        }
        os << "\n";
    }

    return os.str();
}

String PlanPrinter::jsonPipelineProfile(PlanSegmentDescriptions & segment_descs, SegmentAndWorkerToGroupedProfile & worker_grouped_profiles)
{
    Poco::JSON::Object::Ptr distributed_plan = new Poco::JSON::Object(true);
    Poco::JSON::Array segments;
    for (auto & segment_desc : segment_descs)
    {
        Poco::JSON::Object::Ptr segment_json = segment_desc->jsonPlanSegmentDescription({}, true);
        if (worker_grouped_profiles.contains(segment_desc->segment_id))
        {
            Poco::JSON::Object::Ptr worker_profiles_json = new Poco::JSON::Object(true);
            for (auto [woker_ip, profile] : worker_grouped_profiles[segment_desc->segment_id])
                worker_profiles_json->set(woker_ip, profile->getJsonProfiles());
            segment_json->set("profiles", worker_profiles_json);
        }
        segments.add(segment_json);
    }
    distributed_plan->set("PipelineProfiles", segments);
    std::ostringstream os;
    distributed_plan->stringify(os, 1);
    return os.str();
}

void PlanPrinter::getRemoteSegmentId(const QueryPlan::Node * node, std::unordered_map<PlanNodeId, size_t> & exchange_to_segment)
{
    auto * step = dynamic_cast<RemoteExchangeSourceStep *>(node->step.get());
    if (step)
        exchange_to_segment[node->id] = step->getInput()[0]->getPlanSegmentId();

    for (const auto & child : node->children)
        getRemoteSegmentId(child, exchange_to_segment);
}

std::unordered_map<PlanNodeId, PlanNodePtr> PlanPrinter::getPlanNodeMap(const QueryPlan & query_plan)
{
    std::unordered_map<PlanNodeId, PlanNodePtr> id_to_node;
    const auto & plan =  query_plan.getPlanNodeRoot();
    if (!plan)
        return id_to_node;

    id_to_node[plan->getId()] = plan;
    getPlanNodes(plan, id_to_node);

    for (const auto & cte : query_plan.getCTEInfo().getCTEs())
    {
        id_to_node[cte.second->getId()] = cte.second;
        getPlanNodes(cte.second, id_to_node);
    }

    return id_to_node;
}

void PlanPrinter::getPlanNodes(const PlanNodePtr & parent, std::unordered_map<PlanNodeId, PlanNodePtr> & id_to_node)
{
    for (const auto & child : parent->getChildren())
    {
        id_to_node[child->getId()] = child;
        if (!child->getChildren().empty())
            getPlanNodes(child, id_to_node);
    }
}

String PlanPrinter::TextPrinter::printOutputColumns(PlanNodeBase & plan_node, const TextPrinterIntent & intent)
{
    auto header = plan_node.getStep()->getOutputStream().header;

    String res;
    size_t line_feed_limit = 120;
    res += intent.print() + "Output Columns: [";

    std::vector<std::string> output_columns;
    for (auto & it : header)
    {
        output_columns.push_back(it.name);
    }
    sort(output_columns.begin(), output_columns.end());

    bool first = true;
    for (auto & column_name : output_columns) {
        if (res.length() > line_feed_limit) {
            res += "\n";
            res += intent.print() + String(17, ' ');
            line_feed_limit += 120;
            first = true;
        }
        if (first) {
            res += column_name;
            first = false;
        }
        else {
            res += ", ";
            res += column_name;
        }
    }
    res += "]\n";
    return res;
}

TextPrinterIntent TextPrinterIntent::forChild(bool last, bool hasChildren_) const
{
    return TextPrinterIntent{
        next_lines_prefix + (last ? LAST_PREFIX : INTERMEDIATE_PREFIX),
        next_lines_prefix + (last ? EMPTY_PREFIX : VERTICAL_LINE),
        hasChildren_};
}

TextPrinterIntent::TextPrinterIntent(String current_lines_prefix_, String next_lines_prefix_, bool hasChildren_)
    : current_lines_prefix(std::move(current_lines_prefix_)), next_lines_prefix(std::move(next_lines_prefix_)), hasChildren(hasChildren_)
{
}

String TextPrinterIntent::detailIntent() const
{
    return "\n" + next_lines_prefix + (hasChildren ? VERTICAL_LINE : EMPTY_PREFIX) + EMPTY_PREFIX;
}

String PlanPrinter::TextPrinter::printLogicalPlan(PlanNodeBase & plan, const TextPrinterIntent & intent, const StepAggregatedOperatorProfiles & profiles) // NOLINT(misc-no-recursion)
{
    std::stringstream out;

    auto step = plan.getStep();
    if (step->getType() == IQueryPlanStep::Type::ExplainAnalyze)
        return printLogicalPlan(*plan.getChildren()[0], intent, profiles);

    if (profiles.empty())
    {
        if (print_stats)
            out << intent.print() << printPrefix(plan) << step->getName() << printSuffix(plan) << " " << printStatistics(plan, intent)
                << printDetail(plan.getStep(), intent) << "\n";
        else
            out << intent.print() << printPrefix(plan) << step->getName() << printSuffix(plan) << printDetail(plan.getStep(), intent) << "\n";
    }
    else
    {
        if (print_profile)
        {
            out << intent.print() << printPrefix(plan) << step->getName() << printSuffix(plan);
            if (print_stats)
                out << intent.detailIntent() << printStatistics(plan, intent);
            out << printOperatorProfiles(plan, intent, profiles);
            if (profiles.count(plan.getId()))
                out << intent.detailIntent() << printQError(plan, profiles);
            out << printDetail(plan.getStep(), intent) << "\n";
        }
        else
        {
            out << intent.print() << printPrefix(plan) << step->getName() << printSuffix(plan);
            if (print_stats)
                out << intent.detailIntent() << printStatistics(plan, intent);
            out << printDetail(plan.getStep(), intent) << "\n";
        }

    }

    if ((step->getType() == IQueryPlanStep::Type::CTERef || step->getType() == IQueryPlanStep::Type::Exchange) && is_distributed)
        return out.str();

    for (auto it = plan.getChildren().begin(); it != plan.getChildren().end();)
    {
        auto child = *it++;
        bool last = it == plan.getChildren().end();
        bool has_children = !child->getChildren().empty();
        if ((child->getStep()->getType() == IQueryPlanStep::Type::CTERef || child->getStep()->getType() == IQueryPlanStep::Type::Exchange) && is_distributed)
            has_children = false;

        out << printLogicalPlan(*child, intent.forChild(last, has_children), profiles);
    }

    return out.str();
}

String PlanPrinter::TextPrinter::printPipelineProfile(GroupedProcessorProfilePtr & input_root, const TextPrinterIntent & intent)
{
    std::stringstream out;
    out << intent.print() << printProcessorDetail(input_root, intent) << "\n";

    for (auto it = input_root->children.begin(); it != input_root->children.end();)
    {
        auto child = *it++;
        bool last = it == input_root->children.end();
        bool has_children = !child->children.empty() && child->children[0];
        out << printPipelineProfile(child, intent.forChild(last, has_children));
    }
    return out.str();
}

String PlanPrinter::TextPrinter::printProcessorDetail(GroupedProcessorProfilePtr profile, const TextPrinterIntent & intent)
{
    std::stringstream out;
    out << profile->processor_name <<"  x" << profile->parallel_size << " ElapsedTime:" << prettySeconds(profile->grouped_elapsed_us);
    out << intent.detailIntent() << "(Input:[WaitTime:" << prettySeconds(profile->grouped_input_wait_elapsed_us) << " Rows:" << prettyNum(profile->grouped_input_rows) << " (" << prettyBytes(profile->grouped_input_bytes) << ")]"
        << " Output:[WaitTime:" << prettySeconds(profile->grouped_input_wait_elapsed_us) << " Rows:" << prettyNum(profile->grouped_output_rows) << " (" << prettyBytes(profile->grouped_output_bytes) << ")])";
    return out.str();
}

String PlanPrinter::TextPrinter::printStatistics(const PlanNodeBase & plan, const TextPrinterIntent &) const
{
    if (!print_stats)
        return "";
    std::stringstream out;
    const auto & stats = plan.getStatistics();
    out << "Est. " << (stats ? std::to_string(stats.value()->getRowCount()) : "?") << " rows";
    if (costs.contains(plan.getId()))
        out << ", cost " << std::scientific << costs.at(plan.getId());
    return out.str();
}

String PlanPrinter::TextPrinter::printOperatorProfiles(PlanNodeBase & plan, const TextPrinterIntent & intent, const StepAggregatedOperatorProfiles & profiles)
{
    size_t step_id = plan.getId();
    if (profiles.count(step_id))
    {
        const auto & profile = profiles.at(step_id);
        std::stringstream out;
        out << intent.detailIntent() << "Act. Output: " << prettyNum(profile->output_rows) << " rows (" << prettyBytes(profile->output_bytes) << ")"
            << ", Wait Time: " << prettySeconds(profile->max_output_wait_elapsed_us)
            << ", Wall Time: " << prettySeconds(profile->max_elapsed_us);

        int num = 1;
        if (!plan.getChildren().empty() && profile->inputs_profile.contains(plan.getChildren()[0]->getId()))
        {
            for (auto & child : plan.getChildren())
            {
                auto input_profile = profile->inputs_profile[child->getId()];
                if (num == 1)
                    out << intent.detailIntent() << "     Input: ";
                else
                    out << intent.detailIntent() << "            ";

                if (plan.getChildren().size() > 1)
                    out << "source [" << num << "] : ";

                out <<  prettyNum(input_profile.input_rows) << " rows (" << prettyBytes(input_profile.input_bytes) << ")";
                out << ", Wait Time: " << prettySeconds(input_profile.input_wait_elapsed_us);
                ++num;
            }
        }
        else
        {
            for (auto & [id, input_metrics] : profile->inputs_profile)
            {
                if (num == 1)
                    out << intent.detailIntent() << "     Input: ";
                else
                    out << intent.detailIntent() << "            ";

                if (plan.getChildren().size() > 1)
                    out << "source [" << num << "] : ";

                out << "Wait Time: " << prettySeconds(input_metrics.input_wait_elapsed_us);
                ++num;
            }
        }

        return out.str();
    }
    return "";
}

String PlanPrinter::TextPrinter::prettyNum(size_t num)
{
    std::vector<std::string> suffixes{ "", "K", "M", "B", "T" };
    size_t idx = 0;
    auto count = static_cast<double>(num);
    while (count >= 1000 && idx < suffixes.size() - 1)
    {
        idx++;
        count /= static_cast<double>(1000);
    }

    std::stringstream out;
    if (idx == 0)
        out << static_cast<int>(count);
    else
        out << std::fixed << std::setprecision(1) << count << suffixes[idx];
    return out.str();
}

String PlanPrinter::TextPrinter::prettySeconds(size_t us)
{
    std::vector<std::string> suffixes{ " us", " ms", " s" };
    size_t idx = 0;
    auto count = static_cast<double>(us);
    while (count >= 1000 && idx < suffixes.size() - 1)
    {
        idx++;
        count /= static_cast<double>(1000);
    }

    std::stringstream out;
    out << std::fixed << std::setprecision(1) << count << suffixes[idx];
    return out.str();
}

String PlanPrinter::TextPrinter::prettyBytes(size_t bytes)
{
    std::vector<std::string> suffixes{ " Bytes", " KB", " MB", " GB", " TB" };
    size_t idx = 0;
    auto count = static_cast<double>(bytes);
    while (count >= 1024 && idx < suffixes.size() - 1)
    {
        idx++;
        count /= static_cast<double>(1024);
    }

    std::stringstream out;
    out << std::fixed << std::setprecision(1) << count << suffixes[idx];
    return out.str();
}

String PlanPrinter::TextPrinter::printQError(const PlanNodeBase & plan, const StepAggregatedOperatorProfiles & profiles)
{
    const auto & stats = plan.getStatistics();
    std::stringstream out;

    size_t step_id = plan.getId();
    if (profiles.count(step_id))
    {
        const auto& profile = profiles.at(step_id);
        if (plan.getChildren().size() > 1)
        {
            size_t max_input_rows = 0;
            for (const auto & p : plan.getChildren())
            {
                if (profiles.count(p->getId()) == 0)
                    continue;
                max_input_rows = std::max(max_input_rows, profiles.at(p->getId())->output_rows);
            }
            if (max_input_rows == 0)
                out << "Filtered: 0.0%";
            else
            {
                double max_rows = static_cast<double>(max_input_rows);
                double filtered = max_rows > 0 ? (((max_rows - static_cast<double>(profile->output_rows)) * static_cast<double>(100) / max_rows)) : 0.0;
                out << "Filtered: " << std::fixed << std::setprecision(1) << filtered << "%";
            }

        }
        else if (plan.getChildren().size() == 1)
        {
            if (profiles.count(plan.getChildren()[0]->getId()) == 0)
                out << "Filtered: 0.0%";
            else
            {
                auto child_input_rows = static_cast<double>(profiles.at(plan.getChildren()[0]->getId())->output_rows);
                double filtered = child_input_rows > 0 ? ((child_input_rows - static_cast<double>(profile->output_rows)) * static_cast<double>(100) / child_input_rows) : 0.0;
                out << "Filtered: " << std::fixed << std::setprecision(1) << filtered << "%";
            }
        }
        else
        {
            out << "Filtered: 0.0%";
        }

        if (stats && stats.value()->getRowCount() != 0 && profile->output_rows != 0)
        {
            if (profile->output_rows > stats.value()->getRowCount())
                out << ", QError: " << std::fixed << std::setprecision(1) << static_cast<double>(profile->output_rows) / static_cast<double>(stats.value()->getRowCount());
            else
                out << ", QError: " << std::fixed << std::setprecision(1) << static_cast<double>(stats.value()->getRowCount()) / static_cast<double>(profile->output_rows);
        }
        return out.str();
    }
    return "";
}

String PlanPrinter::TextPrinter::printPrefix(PlanNodeBase & plan)
{
    if (plan.getStep()->getType() == IQueryPlanStep::Type::Exchange)
    {
        const auto * exchange = dynamic_cast<const ExchangeStep *>(plan.getStep().get());
        auto f = [](ExchangeMode mode) {
            switch (mode)
            {
                case ExchangeMode::LOCAL_NO_NEED_REPARTITION:
                case ExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                    return "Local ";
                case ExchangeMode::BROADCAST:
                    return "Broadcast ";
                case ExchangeMode::REPARTITION:
                    return "Repartition ";
                case ExchangeMode::GATHER:
                    return "Gather ";
                default:
                    return "";
            }
        };
        return f(exchange->getExchangeMode());
    }

    if (plan.getStep()->getType() == IQueryPlanStep::Type::Join)
    {
        const auto * join = dynamic_cast<const JoinStep *>(plan.getStep().get());
        auto f = [](ASTTableJoin::Kind kind) {
            switch (kind)
            {
                case ASTTableJoin::Kind::Inner:
                    return "Inner ";
                case ASTTableJoin::Kind::Left:
                    return "Left ";
                case ASTTableJoin::Kind::Right:
                    return "Right ";
                case ASTTableJoin::Kind::Full:
                    return "Full ";
                case ASTTableJoin::Kind::Cross:
                    return "Cross ";
                default:
                    return "";
            }
        };
        if (join->getJoinAlgorithm() != JoinAlgorithm::AUTO)
            return fmt::format("{}({}) ", f(join->getKind()), JoinAlgorithmConverter::toString(join->getJoinAlgorithm()));

        return f(join->getKind());
    }
    return "";
}

String PlanPrinter::TextPrinter::printSuffix(PlanNodeBase & plan)
{
    std::stringstream out;
    Int64 segment_id = -1;
    if (is_distributed && exchange_to_segment.contains(plan.getId()))
        segment_id = exchange_to_segment.at(plan.getId());

    if (plan.getStep()->getType() == IQueryPlanStep::Type::TableScan)
    {
        const auto * table_scan = dynamic_cast<const TableScanStep *>(plan.getStep().get());
        out << " " << table_scan->getDatabase() << "." << table_scan->getOriginalTable();
    }
    else if (plan.getStep()->getType() == IQueryPlanStep::Type::Exchange && segment_id != -1)
    {
        out << " segment[" << exchange_to_segment.at(plan.getId()) << "]";
    }
    else if (plan.getStep()->getType() == IQueryPlanStep::Type::CTERef)
    {
        const auto *cte = dynamic_cast<const CTERefStep *>(plan.getStep().get());
        out << "[" << cte->getId() << "]" ;
        if (segment_id != -1)
            out << " <--" << " segment[" << exchange_to_segment.at(plan.getId()) << "]";
    }
    return out.str();
}

String PlanPrinter::TextPrinter::printDetail(QueryPlanStepPtr plan, const TextPrinterIntent & intent) const
{
    std::stringstream out;
    if (verbose && plan->getType() == IQueryPlanStep::Type::Union)
    {
        const auto * union_step = dynamic_cast<const UnionStep *>(plan.get());
        out << intent.detailIntent() << "OutputToInputs: ";

        for (auto iter = union_step->getOutToInputs().begin(); iter != union_step->getOutToInputs().end(); ++iter)
        {
            if (iter != union_step->getOutToInputs().begin())
                out << ", ";
            const auto & output_to_inputs = *iter;
            out << output_to_inputs.first << " = ";
            out << join(output_to_inputs.second, ",", "[", "]");
        }
    }

    if (verbose && plan->getType() == IQueryPlanStep::Type::Join)
    {
        const auto * join_step = dynamic_cast<const JoinStep *>(plan.get());
        out << intent.detailIntent() << "Condition: ";
        if (!join_step->getLeftKeys().empty())
            out << join_step->getLeftKeys()[0] << " == " << join_step->getRightKeys()[0];
        for (size_t i = 1; i < join_step->getLeftKeys().size(); i++)
            out << ", " << join_step->getLeftKeys()[i] << " == " << join_step->getRightKeys()[i];

        if (!ASTEquality::compareTree(join_step->getFilter(), PredicateConst::TRUE_VALUE))
        {
            out << intent.detailIntent() << "Filter: ";
            out << serializeAST(*join_step->getFilter());
        }
        if (!join_step->getRuntimeFilterBuilders().empty())
        {
            std::set<std::string> runtime_filters;
            for (const auto & item : join_step->getRuntimeFilterBuilders())
                runtime_filters.emplace(item.first);
            out << intent.detailIntent() << "Runtime Filters Builder: " << join(runtime_filters, ",", "{", "}");
        }
    }

    if (verbose && plan->getType() == IQueryPlanStep::Type::Sorting)
    {
        const auto *sort = dynamic_cast<const SortingStep *>(plan.get());
        std::vector<String> sort_columns;
        for (const auto & desc : sort->getSortDescription())
            sort_columns.emplace_back(desc.format());
        out << intent.detailIntent() << "Order by: " << join(sort_columns, ", ", "{", "}");

        std::visit(
            overloaded{
                [&](size_t x) {
                    if (x)
                        out << intent.detailIntent() << "Limit: " << x;
                },
                [&](const String & x) { out << intent.detailIntent() << "Limit: " << x; }},
            sort->getLimit());
    }

    if (verbose && plan->getType() == IQueryPlanStep::Type::Limit)
    {
        const auto * limit = dynamic_cast<const LimitStep *>(plan.get());
        out << intent.detailIntent();

        std::visit([&](const auto & v) { out << "Limit: " << v; }, limit->getLimit());
        std::visit(
            [&](const auto & v) {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, size_t>)
                {
                    if (v)
                        out << " Offset: " << v;
                }
                else
                {
                    out << " Offset: " << v;
                }
            },
            limit->getOffset());
    }

    if (verbose && plan->getType() == IQueryPlanStep::Type::Offset)
    {
        const auto * offset = dynamic_cast<const OffsetStep *>(plan.get());
        out << intent.detailIntent();
        if (offset->getOffset())
            out << " Offset: " << offset->getOffset();
    }

    if (verbose && plan->getType() == IQueryPlanStep::Type::Aggregating)
    {
        const auto * agg = dynamic_cast<const AggregatingStep *>(plan.get());
        auto keys = agg->getKeys();
        out << intent.detailIntent() << "Group by: " << join(keys, ", ", "{", "}");


        auto keys_not_hashed = agg->getKeysNotHashed();
        if (!keys_not_hashed.empty())
        {
            NameOrderedSet sorted_names(keys_not_hashed.begin(), keys_not_hashed.end());
            out << intent.detailIntent() << "Group by keys not hashed: " << join(sorted_names, ", ", "{", "}");
        }

        std::vector<String> aggregates;
        for (const auto & desc : agg->getAggregates())
        {
            std::stringstream ss;
            String func_name = desc.function->getName();
            auto type_name = String(typeid(desc.function.get()).name());
            if (type_name.find("AggregateFunctionNull"))
                func_name = String("AggNull(").append(std::move(func_name)).append(")");
            ss << desc.column_name << ":=" << func_name << join(desc.argument_names, "," ,"(", ")");
            aggregates.emplace_back(ss.str());
        }
        if (!aggregates.empty())
            out << intent.detailIntent() << "Aggregates: " << join(aggregates, ", ");
    }

    if (verbose && plan->getType() == IQueryPlanStep::Type::Exchange)
    {
        const auto * exchange = dynamic_cast<const ExchangeStep *>(plan.get());
        if (exchange->getExchangeMode() == ExchangeMode::REPARTITION)
        {
            auto keys = exchange->getSchema().getPartitioningColumns();
            out << intent.detailIntent() << "Partition by: " << join(keys, ", ", "{", "}");
        }
    }

    if (verbose && plan->getType() == IQueryPlanStep::Type::Filter)
    {
        const auto * filter = dynamic_cast<const FilterStep *>(plan.get());
        auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter->getFilter());
        out << intent.detailIntent() << "Condition: " << printFilter(filter->getFilter());
    }

    if (verbose && plan->getType() == IQueryPlanStep::Type::Projection)
    {
        const auto * projection = dynamic_cast<const ProjectionStep *>(plan.get());

        std::vector<String> identities;
        std::vector<String> assignments;

        for (const auto & assignment : projection->getAssignments())
            if (Utils::isIdentity(assignment))
                identities.emplace_back(assignment.first);
            else
                assignments.emplace_back(assignment.first + ":=" + serializeAST(*assignment.second));

        std::sort(assignments.begin(), assignments.end());
        if (!identities.empty())
        {
            std::stringstream ss;
            std::sort(identities.begin(), identities.end());
            ss << join(identities, ", ", "[", "]");
            assignments.insert(assignments.begin(), ss.str());
        }

        out << intent.detailIntent() << "Expressions: " << join(assignments, ", ");
    }

    if (verbose && plan->getType() == IQueryPlanStep::Type::TableScan)
    {
        const auto * table_scan = dynamic_cast<const TableScanStep *>(plan.get());
        std::vector<String> identities;
        std::vector<String> assignments;
        for (const auto & name_with_alias : table_scan->getColumnAlias())
            if (name_with_alias.second == name_with_alias.first)
                identities.emplace_back(name_with_alias.second);
            else
                assignments.emplace_back(name_with_alias.second + ":=" + name_with_alias.first);

        const auto & query_info = table_scan->getQueryInfo();
        auto *query = query_info.query->as<ASTSelectQuery>();

        if (query_info.partition_filter)
        {
            out << intent.detailIntent();
            out << "Partition filter: ";
            out << serializeAST(*query_info.partition_filter);
        }

        if (auto where = query->getWhere())
            out << intent.detailIntent() << "Where: " << printFilter(where);
        if (auto prewhere = query->getPrewhere())
            out << intent.detailIntent() << "Prewhere: " << printFilter(prewhere);
        if (query->getLimitLength())
        {
            out << intent.detailIntent() << "Limit: ";
            Field converted = convertFieldToType(query->refLimitLength()->as<ASTLiteral>()->value, DataTypeUInt64());
            out << converted.safeGet<UInt64>();
        }

        std::vector<String> inline_expressions;
        for (const auto & assignment : table_scan->getInlineExpressions())
            inline_expressions.emplace_back(assignment.first + ":=" + serializeAST(*assignment.second));
        if (!inline_expressions.empty())
            out << intent.detailIntent() << "Inline expressions: " << join(inline_expressions, ", ", "[", "]");

        if (!identities.empty())
        {
            std::stringstream ss;
            ss << join(identities, ", ", "[", "]");
            assignments.insert(assignments.begin(), ss.str());
        }

        out << intent.detailIntent() << "Outputs: " << join(assignments, ", ");

        if (table_scan->getPushdownFilter())
            out << printDetail(table_scan->getPushdownFilter(), intent);

        if (table_scan->getPushdownProjection())
            out << printDetail(table_scan->getPushdownProjection(), intent);

        if (table_scan->getPushdownAggregation())
            out << printDetail(table_scan->getPushdownAggregation(), intent);
    }

    if (verbose && plan->getType() == IQueryPlanStep::Type::TopNFiltering)
    {
        const auto *topn_filter = dynamic_cast<const TopNFilteringStep *>(plan.get());
        std::vector<String> sort_columns;
        for (const auto & desc : topn_filter->getSortDescription())
            sort_columns.emplace_back(desc.format());
        out << intent.detailIntent() << "Order by: " << join(sort_columns, ", ", "{", "}");
        out << intent.detailIntent() << "Size: " << topn_filter->getSize();
        out << intent.detailIntent() << "Algorithm: " << TopNFilteringAlgorithmConverter::toString(topn_filter->getAlgorithm());
    }

    if (verbose && plan->getType() == IQueryPlanStep::Type::TableWrite)
    {
        const auto * table_write = dynamic_cast<const TableWriteStep *>(plan.get());
        if (table_write->getTarget())
            out << intent.detailIntent() << table_write->getTarget()->toString();
    }

    return out.str();
}

String PlanPrinter::TextPrinter::printFilter(ConstASTPtr filter)
{
    std::stringstream out;
    auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter);

    if (!filters.second.empty())
        out << serializeAST(*PredicateUtils::combineConjuncts(filters.second));

    if (!filters.first.empty())
    {
        std::set<String> runtime_filters;
        for (auto & item : filters.first)
        {
            auto desc = RuntimeFilterUtils::extractDescription(item).value();
            runtime_filters.emplace(serializeAST(*desc.expr->clone()));
        }
        if (!filters.second.empty())
            out << " ";
        out << "Runtime Filters: "<< join(runtime_filters, ", ", "{", "}");
    }

    if (filters.first.empty() && filters.second.empty())
        out << "True";
    return out.str();
}

void NodeDescription::setStepDetail(QueryPlanStepPtr step)
{
    type = step->getType();
    step_name = step->getName();
    if (step->getType() == IQueryPlanStep::Type::Union)
    {
        const auto * union_step = dynamic_cast<const UnionStep *>(step.get());
        for (const auto & output_to_inputs : union_step->getOutToInputs())
        {
            step_vector_detail["OutputToInputs"].emplace_back(output_to_inputs.first + " = " + join(output_to_inputs.second, ",", "[", "]"));
        }
    }

    if (step->getType() == IQueryPlanStep::Type::Join)
    {
        const auto * join_step = dynamic_cast<const JoinStep *>(step.get());
        auto get_kind = [](ASTTableJoin::Kind kind) {
            switch (kind)
            {
                case ASTTableJoin::Kind::Inner:
                    return "Inner";
                case ASTTableJoin::Kind::Left:
                    return "Left";
                case ASTTableJoin::Kind::Right:
                    return "Right";
                case ASTTableJoin::Kind::Full:
                    return "Full";
                case ASTTableJoin::Kind::Cross:
                    return "Cross";
                default:
                    return "";
            }
        };
        auto get_strictness = [](ASTTableJoin::Strictness strictness) {
            switch (strictness)
            {
                case ASTTableJoin::Strictness::RightAny:
                    return "RightAny ";
                case ASTTableJoin::Strictness::Any:
                    return "Any ";
                case ASTTableJoin::Strictness::Asof:
                    return "Asof ";
                case ASTTableJoin::Strictness::Semi:
                    return "Semi ";
                case ASTTableJoin::Strictness::Anti:
                    return "Anti";
                default:
                    return "";
            }
        };
        step_detail["JoinKind"] = get_kind(join_step->getKind());
        step_detail["Strictness"] = get_strictness(join_step->getStrictness());
        if (join_step->getJoinAlgorithm() != JoinAlgorithm::AUTO)
            step_detail["Algorithm"] = JoinAlgorithmConverter::toString(join_step->getJoinAlgorithm());

        String condition;
        for (size_t i = 0; i < join_step->getLeftKeys().size(); i++)
            step_vector_detail["Condition"].emplace_back(join_step->getLeftKeys()[i] + " == " + join_step->getRightKeys()[i]);

        if (!ASTEquality::compareTree(join_step->getFilter(), PredicateConst::TRUE_VALUE))
            step_detail["Filter"] = serializeAST(*join_step->getFilter());

        if (!join_step->getRuntimeFilterBuilders().empty())
        {
            std::set<std::string> runtime_filters;
            for (const auto & item : join_step->getRuntimeFilterBuilders())
                step_vector_detail["RuntimeFiltersBuilder"].emplace_back(item.first);
        }
    }

    if (step->getType() == IQueryPlanStep::Type::Sorting)
    {
        const auto * sort = dynamic_cast<const SortingStep *>(step.get());
        std::vector<String> sort_columns;
        for (const auto & desc : sort->getSortDescription())
            step_vector_detail["OrderBy"].emplace_back(
                desc.column_name + (desc.direction == -1 ? " desc" : " asc") + (desc.nulls_direction == -1 ? " nulls_last" : ""));
        std::visit(
            overloaded{
                [&](size_t x) {
                    if (x)
                        step_detail["Limit"] = std::to_string(x);
                },
                [&](const String & x) { step_detail["Limit"] = x; }},
            sort->getLimit());
    }

    if (step->getType() == IQueryPlanStep::Type::Limit)
    {
        const auto * limit = dynamic_cast<const LimitStep *>(step.get());
        std::visit(
            [&](const auto & e) {
                using T = std::decay_t<decltype(e)>;
                if constexpr (std::is_same_v<T, size_t>)
                    step_detail["Limit"] = std::to_string(e);
                else
                    step_detail["Limit"] = e;
            },
            limit->getLimit());
        std::visit(
            [&](const auto & e) {
                using T = std::decay_t<decltype(e)>;
                if constexpr (std::is_same_v<T, size_t>)
                    step_detail["Offset"] = std::to_string(e);
                else
                    step_detail["Offset"] = e;
            },
            limit->getOffset());
    }

        if (step->getType() == IQueryPlanStep::Type::Offset)
    {
        const auto * offset = dynamic_cast<const OffsetStep *>(step.get());
        if (offset->getOffset())
            step_detail["Offset"] = std::to_string(offset->getOffset());
    }

    if (step->getType() == IQueryPlanStep::Type::Aggregating)
    {
        const auto * agg = dynamic_cast<const AggregatingStep *>(step.get());
        auto keys = agg->getKeys();
        for (auto & key : keys)
            step_vector_detail["GroupByKeys"].emplace_back(key);

        auto keys_not_hashed = agg->getKeysNotHashed();
        if (!keys_not_hashed.empty())
        {
            for (const auto & key : keys_not_hashed)
                step_vector_detail["GroupByKeysNotHashed"].emplace_back(key);
        }

        for (const auto & desc : agg->getAggregates())
        {
            std::stringstream ss;
            String func_name = desc.function->getName();
            auto type_name = String(typeid(desc.function.get()).name());
            if (type_name.find("AggregateFunctionNull") != String::npos)
                func_name = String("AggNull(").append(std::move(func_name)).append(")");
            ss << desc.column_name << ":=" << func_name << join(desc.argument_names, ",", "(", ")");
            step_vector_detail["Aggregates"].emplace_back(ss.str());
        }
    }
    if (step->getType() == IQueryPlanStep::Type::MergingAggregated)
    {
        const auto * agg = dynamic_cast<const MergingAggregatedStep *>(step.get());
        auto keys = agg->getKeys();
        for (auto & key : keys)
            step_vector_detail["GroupByKeys"].emplace_back(key);

        for (const auto & desc : agg->getAggregates())
        {
            std::stringstream ss;
            String func_name = desc.function->getName();
            auto type_name = String(typeid(desc.function.get()).name());
            if (type_name.find("AggregateFunctionNull") != String::npos)
                func_name = String("AggNull(").append(std::move(func_name)).append(")");
            ss << desc.column_name << ":=" << func_name << join(desc.argument_names, ",", "(", ")");
            step_vector_detail["Aggregates"].emplace_back(ss.str());
        }
    }

    if (step->getType() == IQueryPlanStep::Type::Exchange)
    {
        const auto * exchange = dynamic_cast<const ExchangeStep *>(step.get());
        auto f = [](ExchangeMode mode) {
            switch (mode)
            {
                case ExchangeMode::LOCAL_NO_NEED_REPARTITION:
                case ExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                    return "Local";
                case ExchangeMode::BROADCAST:
                    return "Broadcast";
                case ExchangeMode::REPARTITION:
                    return "Repartition";
                case ExchangeMode::GATHER:
                    return "Gather";
                default:
                    return "";
            }
        };
        step_detail["Mode"] = f(exchange->getExchangeMode());
        if (exchange->getExchangeMode() == ExchangeMode::REPARTITION)
        {
            for (const auto & item : (exchange->getSchema().getPartitioningColumns()))
                step_vector_detail["PartitionBy"].emplace_back(item);
        }
    }

    if (step->getType() == IQueryPlanStep::Type::Filter)
    {
        const auto * filter = dynamic_cast<const FilterStep *>(step.get());
        auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter->getFilter());
        step_detail["Filter"] = serializeAST(*PredicateUtils::combineConjuncts(filters.second));
        if (!filters.first.empty())
            step_detail["RuntimeFilter"] = serializeAST(*PredicateUtils::combineConjuncts(filters.first));
    }

    if (step->getType() == IQueryPlanStep::Type::Projection)
    {
        const auto * projection = dynamic_cast<const ProjectionStep *>(step.get());

        std::vector<String> identities;
        std::vector<String> assignments;

        for (const auto & assignment : projection->getAssignments())
            if (Utils::isIdentity(assignment))
                identities.emplace_back(assignment.first);
            else
                assignments.emplace_back(assignment.first + ":=" + serializeAST(*assignment.second));

        std::sort(assignments.begin(), assignments.end());
        if (!identities.empty())
        {
            std::stringstream ss;
            std::sort(identities.begin(), identities.end());
            for (auto & identitie : identities)
                assignments.insert(assignments.begin(), identitie);
        }
        for (auto & assignment : assignments)
            step_vector_detail["Expressions"].emplace_back(assignment);
    }

    if (step->getType() == IQueryPlanStep::Type::TableScan)
    {
        const auto * table_scan = dynamic_cast<const TableScanStep *>(step.get());
        std::vector<String> identities;
        std::vector<String> assignments;
        for (const auto & name_with_alias : table_scan->getColumnAlias())
            if (name_with_alias.second == name_with_alias.first)
                identities.emplace_back(name_with_alias.second);
            else
                assignments.emplace_back(name_with_alias.second + ":=" + name_with_alias.first);

        const auto & query_info = table_scan->getQueryInfo();
        auto * query = query_info.query->as<ASTSelectQuery>();

        if (auto where = query->getWhere())
            step_detail["Where"] = PlanPrinter::TextPrinter::printFilter(where);
        if (auto prewhere = query->getPrewhere())
            step_detail["Prewhere"] = PlanPrinter::TextPrinter::printFilter(prewhere);
        if (query->getLimitLength())
        {
            Field converted = convertFieldToType(query->refLimitLength()->as<ASTLiteral>()->value, DataTypeUInt64());
            step_detail["Limit"] = std::to_string(converted.safeGet<UInt64>());
        }

        if (!identities.empty())
        {
            std::stringstream ss;
            for (auto & identitie : identities)
                assignments.insert(assignments.begin(), identitie);
        }

        for (auto & assignment : assignments)
            step_vector_detail["Outputs"].emplace_back(assignment);

        std::vector<String> inline_expressions;
        for (const auto & assignment : table_scan->getInlineExpressions())
            step_vector_detail["InlineExpressions"].emplace_back(assignment.first + ":=" + serializeAST(*assignment.second));

        if (table_scan->getPushdownFilter())
        {
            NodeDescriptionPtr push_down_filter_detail = std::make_shared<NodeDescription>();
            push_down_filter_detail->setStepDetail(table_scan->getPushdownFilter());
            descriptions_in_step["PushDownFilter"] = push_down_filter_detail;
        }

        if (table_scan->getPushdownProjection())
        {
            NodeDescriptionPtr push_down_projection_detail = std::make_shared<NodeDescription>();
            push_down_projection_detail->setStepDetail(table_scan->getPushdownProjection());
            descriptions_in_step["PushDownProjection"] = push_down_projection_detail;
        }

        if (table_scan->getPushdownAggregation())
        {
            NodeDescriptionPtr push_down_aggregation_detail = std::make_shared<NodeDescription>();
            push_down_aggregation_detail->setStepDetail(table_scan->getPushdownAggregation());
            descriptions_in_step["PushDownAggregation"] = push_down_aggregation_detail;
        }
    }

    if (step->getType() == IQueryPlanStep::Type::TopNFiltering)
    {
        const auto *topn_filter = dynamic_cast<const TopNFilteringStep *>(step.get());
        std::vector<String> sort_columns;
        for (const auto & desc : topn_filter->getSortDescription())
            step_vector_detail["OrderBy"].emplace_back(desc.format());
        step_detail["Size"] = std::to_string(topn_filter->getSize());
    }

    if (step->getType() == IQueryPlanStep::Type::TableWrite)
    {
        const auto * table_write = dynamic_cast<const TableWriteStep *>(step.get());
        if (table_write->getTarget())
            step_detail["Target"] = table_write->getTarget()->toString();
    }

    if (step->getType() == IQueryPlanStep::Type::CTERef)
    {
        const auto * cte = dynamic_cast<const CTERefStep *>(step.get());
        step_detail["CTEId"] = std::to_string(cte->getId());
    }

    if (step->getType() == IQueryPlanStep::Type::RemoteExchangeSource)
    {
        const auto * remote_write = dynamic_cast<const RemoteExchangeSourceStep *>(step.get());
        auto inputs = remote_write->getInput();
        for (const auto & input : inputs)
        {
            for (const auto & column : input->getHeader())
                step_vector_detail["Segment["+ std::to_string(input->getPlanSegmentId())+"]"].emplace_back(column.name);
        }
    }
}

void NodeDescription::setStepStatistic(PlanNodePtr node)
{
    if (node->getStatistics().has_value())
    {
        NodeDescription::StatisticInfo node_stats;
        node_stats.row_count = node->getStatistics().value()->getRowCount();
        stats = node_stats;
    }
}

Poco::JSON::Object::Ptr NodeDescription::jsonNodeDescription(const StepAggregatedOperatorProfiles & node_profiles, bool print_stats)
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(true);
    json->set("NodeId", node_id);
    json->set("NodeType", step_name);
    for (auto & detail : step_detail)
        json->set(detail.first, detail.second);
    for (auto & vector_detail : step_vector_detail)
    {
        Poco::JSON::Array details;
        for (const auto& item : vector_detail.second)
            details.add(item);
        json->set(vector_detail.first, details);
    }

    if (stats.has_value() && print_stats)
    {
        Poco::JSON::Object::Ptr stats_json = new Poco::JSON::Object(true);
        stats_json->set("RowCount", stats.value().row_count);
        json->set("Statistic", stats_json);
    }

    if (node_profiles.contains(node_id))
    {
        const auto & profile_detail = node_profiles.at(node_id);
        Poco::JSON::Object::Ptr profiles = new Poco::JSON::Object(true);
        profiles->set("WallTimeMs", profile_detail->max_elapsed_us/1000);
        profiles->set("OutputRows", profile_detail->output_rows);
        profiles->set("OutputBytes", profile_detail->output_bytes);
        profiles->set("OutputWaitTimeMs", profile_detail->max_output_wait_elapsed_us/1000);
        Poco::JSON::Array inputs_profile;
        for (auto input_profile : profile_detail->inputs_profile)
        {
            Poco::JSON::Object::Ptr input = new Poco::JSON::Object(true);
            input->set("InputNodeId", input_profile.first);
            input->set("InputRows", input_profile.second.input_rows);
            input->set("InputBytes", input_profile.second.input_bytes);
            input->set("InputWaitTimeMs", input_profile.second.input_wait_elapsed_us/1000);
            inputs_profile.add(input);
        }
        profiles->set("Inputs", inputs_profile);

        double filtered = 0.0;
        if (children.size() > 1)
        {
            size_t max_input_rows = 0;
            for (const auto & child : children)
            {
                if (!node_profiles.contains(child->node_id))
                    continue;
                max_input_rows = std::max(max_input_rows, node_profiles.at(child->node_id)->output_rows);
            }
            if (max_input_rows != 0)
            {
                double max_rows = static_cast<double>(max_input_rows);
                filtered = max_rows > 0 ? ((max_rows - static_cast<double>(profile_detail->output_rows)) * static_cast<double>(100) / max_rows) : 0;
            }
        }
        else if (children.size() == 1)
        {
            if (node_profiles.contains(children[0]->node_id))
            {
                auto child_input_rows = static_cast<double>(node_profiles.at(children[0]->node_id)->output_rows);
                filtered = child_input_rows > 0 ? ((child_input_rows - static_cast<double>(profile_detail->output_rows)) * static_cast<double>(100) / child_input_rows) : 0;
            }
        }
        profiles->set("FilteredRate", filtered);
        json->set("Profiles", profiles);
    }

    if (!descriptions_in_step.empty())
    {
        Poco::JSON::Object::Ptr descriptions = new Poco::JSON::Object(true);
        for (auto & desc : descriptions_in_step)
            descriptions->set(desc.first, desc.second->jsonNodeDescription(node_profiles, print_stats));
        json->set("StepDescriptions", descriptions);
    }
    Poco::JSON::Array children_array;
    for (auto & child : children)
        children_array.add(child->jsonNodeDescription(node_profiles, print_stats));

    if (!children.empty())
        json->set("Children", children_array);
    return json;
}

NodeDescriptionPtr NodeDescription::getPlanDescription(QueryPlan::Node * node)
{
    auto description = std::make_shared<NodeDescription>();
    description->node_id = node->id;
    description->setStepDetail(node->step);
    for (auto * child : node->children)
    {
        auto child_desc = getPlanDescription(child);
        description->children.emplace_back(child_desc);
    }
    return description;
}

NodeDescriptionPtr NodeDescription::getPlanDescription(PlanNodePtr node)
{
    auto description = std::make_shared<NodeDescription>();
    description->node_id = node->getId();
    description->setStepDetail(node->getStep());
    description->setStepStatistic(node);
    for (auto & child : node->getChildren())
    {
        auto child_desc = getPlanDescription(child);
        description->children.emplace_back(child_desc);
    }
    return description;
}

String PlanSegmentDescription::jsonPlanSegmentDescriptionAsString(const StepAggregatedOperatorProfiles & profiles)
{
    auto json = jsonPlanSegmentDescription(profiles);
    std::ostringstream os;
    json->stringify(os, 1);
    return os.str();
}

Poco::JSON::Object::Ptr PlanSegmentDescription::jsonPlanSegmentDescription(const StepAggregatedOperatorProfiles & profiles, bool is_pipeline)
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(true);

    auto f = [](ExchangeMode xchg_mode) {
        switch (xchg_mode)
        {
            case ExchangeMode::LOCAL_NO_NEED_REPARTITION:
                return "LOCAL_NO_NEED_REPARTITION";
            case ExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                return "LOCAL_MAY_NEED_REPARTITION";
            case ExchangeMode::BROADCAST:
                return "BROADCAST";
            case ExchangeMode::REPARTITION:
                return "REPARTITION";
            case ExchangeMode::GATHER:
                return "GATHER";
            default:
                return "UNKNOWN";
        }
    };

    json->set("SegmentID", segment_id);
    json->set("SegmentType", segment_type);
    String exchange = (segment_id == 0) ? "Output" : f(mode);
    json->set("OutputExchangeMode", exchange);
    if (exchange == "REPARTITION") // print shuffle keys
    {
        Poco::JSON::Array keys;
        for (auto & key : shuffle_keys)
            keys.add(key);
        json->set("ShuffleKeys", exchange);
    }
    json->set("ParallelSize", parallel);
    json->set("ClusterName", (cluster_name.empty() ? "server" : cluster_name));
    json->set("ExchangeParallelSize", exchange_parallel_size);
    if (!output_columns.empty())
    {
        Poco::JSON::Array output_array;
        for (const auto & column : output_columns)
            output_array.add(column);
        json->set("OutputColumns", output_array);
    }

    if (!outputs_desc.empty())
    {
        Poco::JSON::Array outputs;
        for (auto & output : outputs_desc)
        {
            Poco::JSON::Object::Ptr output_json = new Poco::JSON::Object(true);
            output_json->set("SegmentID",output->segment_id);
            output_json->set("PlanSegmentType",output->plan_segment_type);
            output_json->set("ParallelSize",output->parallel_size);
            outputs.add(output_json);
        }
        json->set("Outputs", outputs);
    }

    if (node_description && !is_pipeline)
        json->set("QueryPlan", node_description->jsonNodeDescription(profiles, false));
    return json;
}

PlanSegmentDescriptionPtr PlanSegmentDescription::getPlanSegmentDescription(PlanSegmentPtr & segment, bool record_plan_detail)
{
    auto plan_segment_desc = std::make_shared<PlanSegmentDescription>();
    auto & query_plan = segment->getQueryPlan();
    plan_segment_desc->segment_id = segment->getPlanSegmentId();
    plan_segment_desc->root_id = query_plan.getRoot()->id;
    plan_segment_desc->root_child_id = query_plan.getRoot()->children.empty() ? query_plan.getRoot()->id : query_plan.getRoot()->children[0]->id;
    plan_segment_desc->query_id = segment->getQueryId();
    plan_segment_desc->cluster_name = segment->getClusterName();
    plan_segment_desc->parallel = segment->getParallelSize();
    plan_segment_desc->exchange_parallel_size = segment->getExchangeParallelSize();
    plan_segment_desc->shuffle_keys = segment->getPlanSegmentOutput()->getShufflekeys();
    plan_segment_desc->mode = segment->getPlanSegmentOutput()->getExchangeMode();
    std::unordered_map<PlanNodeId, size_t> exchange_to_segment;
    segment->getRemoteSegmentId(query_plan.getRoot(), exchange_to_segment);
    plan_segment_desc->exchange_to_segment = exchange_to_segment;

    if (plan_segment_desc->segment_id == 0)
        plan_segment_desc->segment_type = "OUTPUT";
    else if (plan_segment_desc->exchange_to_segment.empty())
        plan_segment_desc->segment_type = "SOURCE";
    else
        plan_segment_desc->segment_type = "PROCESS";

    if (segment->getPlanSegmentId() != 0)
    {
        for (auto & output : segment->getPlanSegmentOutputs())
        {
            PlanSegmentDescription::OutputInfo output_desc;
            output_desc.segment_id = output->getPlanSegmentId();
            output_desc.plan_segment_type = planSegmentTypeToString(output->getPlanSegmentType());
            output_desc.parallel_size = output->getParallelSize();
            auto output_desc_ptr = std::make_shared<PlanSegmentDescription::OutputInfo>(output_desc);
            plan_segment_desc->outputs_desc.emplace_back(output_desc_ptr);
        }
    }

    if (query_plan.getRoot())
    {
        const auto & header = query_plan.getRoot()->step->getOutputStream().header;
        for (const auto & it : header)
            plan_segment_desc->output_columns.push_back(it.name);
    }

    if (record_plan_detail)
        plan_segment_desc->node_description = NodeDescription::getPlanDescription(query_plan.getRoot());
    return plan_segment_desc;
}

String PlanPrinter::jsonDistributedPlan(PlanSegmentDescriptions & segment_descs, const StepAggregatedOperatorProfiles & profiles)
{
    Poco::JSON::Object::Ptr distributed_plan = new Poco::JSON::Object(true);
    Poco::JSON::Array segments;
    for (auto & segment_desc : segment_descs)
        segments.add(segment_desc->jsonPlanSegmentDescription(profiles));
    distributed_plan->set("DistributedPlan", segments);
    std::ostringstream os;
    distributed_plan->stringify(os, 1);
    return os.str();
}

}
