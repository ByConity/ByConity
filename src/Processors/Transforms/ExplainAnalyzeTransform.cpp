#include <set>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InterpreterExplainQuery.h>
#include <Interpreters/SegmentScheduler.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/CostModel/CostCalculator.h>
#include <Processors/Transforms/ExplainAnalyzeTransform.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/PlanPrinter.h>
#include "Interpreters/ProcessorProfile.h"

namespace DB
{

ExplainAnalyzeTransform::ExplainAnalyzeTransform(
    const Block & header_,
    ASTExplainQuery::ExplainKind kind_,
    std::shared_ptr<QueryPlan> query_plan_ptr_,
    ContextMutablePtr context_,
    PlanSegmentDescriptions & segment_descriptions_,
    QueryPlanSettings settings_)
    : ISimpleTransform(header_, {{std::make_shared<DataTypeString>(),"Explain Analyze"}}, true)
    , kind(kind_)
    , context(context_)
    , query_plan_ptr(std::move(query_plan_ptr_))
    , segment_descriptions(segment_descriptions_)
    , settings(settings_)
{
    coordinator_address = extractExchangeHostPort(context->getQueryContext()->getCoordinatorAddress());
}

void ExplainAnalyzeTransform::transform(Chunk & chunk)
{
    chunk.clear();
    if (!input.isFinished())
        return;

    // If the information of segment0 cannot be accepted
    ProcessorsSet processors_set;
    ProcessorProfiles profiles;
    getProcessorProfiles(processors_set, profiles, this);
    auto segment0_profile = GroupedProcessorProfile::getGroupedProfiles(profiles);

    auto scheduler = context->getSegmentScheduler();
    UInt64 time_out = context->getSettingsRef().operator_profile_receive_timeout;
    auto time_start = std::chrono::system_clock::now();
    while (!scheduler->alreadyReceivedAllSegmentStatus(context->getCurrentQueryId()))
    {
        auto now = std::chrono::system_clock::now();
        UInt64 elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - time_start).count();
        if (elapsed >= time_out)
            break;
    }

    auto profiles_map = scheduler->getSegmentsProfile(context->getCurrentQueryId());
    String explain;
    if ((kind == ASTExplainQuery::ExplainKind::LogicalAnalyze || kind == ASTExplainQuery::ExplainKind::DistributedAnalyze))
    {
        AddressToStepProfile addr_to_step_profile;
        for (auto & [segment_id, segment_profiles] : profiles_map)
        {
            for (auto & segment_profile : segment_profiles)
            {
                for (auto & [step_id, profile] : segment_profile->profiles)
                    addr_to_step_profile[segment_profile->worker_address][step_id] = profile;
            }
        }

        auto segment0_steps_profiles = GroupedProcessorProfile::aggregateOperatorProfileToStepLevel(segment0_profile);
        for (auto & [step_id, profile] : segment0_steps_profiles)
            addr_to_step_profile[coordinator_address][step_id] = profile;

        CardinalityEstimator::estimate(*query_plan_ptr, context);
        std::unordered_map<PlanNodeId, double> costs = CostCalculator::calculate(*query_plan_ptr, *context);
        auto step_agg_operator_profiles = ProfileMetric::aggregateStepProfileBetweenWorkers(addr_to_step_profile);
        if (kind == ASTExplainQuery::ExplainKind::LogicalAnalyze)
        {
            if (settings.json)
            {
                auto plan_cost = CostCalculator::calculatePlanCost(*query_plan_ptr, *context);
                explain = PlanPrinter::jsonLogicalPlan(*query_plan_ptr, plan_cost, CostModel(*context), step_agg_operator_profiles, costs, settings);
            }
            else
                explain = PlanPrinter::textLogicalPlan(*query_plan_ptr, context, costs, step_agg_operator_profiles, settings);
        }
        else if (kind == ASTExplainQuery::ExplainKind::DistributedAnalyze && !segment_descriptions.empty())
        {
            if (settings.json)
                explain = PlanPrinter::jsonDistributedPlan(segment_descriptions, step_agg_operator_profiles);
            else
                explain = PlanPrinter::textDistributedPlan(
                    segment_descriptions, context, costs, step_agg_operator_profiles, *query_plan_ptr, settings, profiles_map);
        }
        GraphvizPrinter::printLogicalPlan(*query_plan_ptr, context, "5999_explain_analyze", step_agg_operator_profiles);
    }
    else if (kind == ASTExplainQuery::ExplainKind::PipelineAnalyze)
    {
        SegIdAndAddrToPipelineProfile worker_grouped_profiles;
        segment0_profile = GroupedProcessorProfile::getOutputRoot(segment0_profile);
        if (segment0_profile->processor_name == "output_root" && !segment0_profile->children.empty())
            segment0_profile = segment0_profile->children[0];
        worker_grouped_profiles[0][coordinator_address] = segment0_profile;
        for (auto & [segment_id, segment_profiles] : profiles_map)
        {
            for (auto & segment_profile : segment_profiles)
            {
                if (segment_profile->profiles.empty())
                    continue;
                auto profile
                    = GroupedProcessorProfile::getGroupedProfileFromMetrics(segment_profile->profiles, segment_profile->profile_root_id);
                if (profile->processor_name == "output_root" && !profile->children.empty())
                    profile = profile->children[0];
                worker_grouped_profiles[segment_profile->segment_id][segment_profile->worker_address] = std::move(profile);
            }
        }

        if (settings.aggregate_profiles)
            worker_grouped_profiles = GroupedProcessorProfile::aggregatePipelineProfileBetweenWorkers(worker_grouped_profiles);
        if (settings.json)
            explain = PlanPrinter::jsonPipelineProfile(segment_descriptions, worker_grouped_profiles);
        else
            explain = PlanPrinter::textPipelineProfile(segment_descriptions, worker_grouped_profiles, settings, profiles_map);
    }

    MutableColumns cols(1);
    auto type = std::make_shared<DataTypeString>();
    cols[0] = type->createColumn();
    InterpreterExplainQuery::fillColumn(*cols[0], explain);
    size_t row_num = cols[0]->size();
    has_final_transform = false;
    has_output = true;
    chunk.setColumns(std::move(cols), row_num);
}

ISimpleTransform::Status ExplainAnalyzeTransform::prepare()
{
    /// Check can output.

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (has_output)
    {
        output.pushData(std::move(output_data));
        has_output = false;

        if (!no_more_data_needed)
            return Status::PortFull;
    }

    /// Stop if don't need more data.
    if (no_more_data_needed)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    /// Check can input.
    if (!has_input)
    {
        if (input.isFinished())
        {
            if (has_final_transform)
                return Status::Ready;
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        input_data = input.pullData(set_input_not_needed_after_read);
        has_input = true;

        if (input_data.exception)
            /// No more data needed. Exception will be thrown (or swallowed) later.
            input.setNotNeeded();
    }

    /// Now transform.
    return Status::Ready;
}

void ExplainAnalyzeTransform::getRemoteProcessorProfiles(std::unordered_map<size_t, std::unordered_map<String, ProcessorProfiles>> & segment_profiles)
{
    // Get operator profile of other segments
    UInt64 time_out = context->getSettingsRef().operator_profile_receive_timeout;
    auto time_start = std::chrono::system_clock::now();

    auto consumer = context->getProcessorProfileElementConsumer();
    while (!consumer->isFinish())
    {
        auto now = std::chrono::system_clock::now();
        UInt64 elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - time_start).count();
        if (elapsed >= time_out)
        {
            consumer->stop();
            break;
        }
    }

    auto remote_profiles = dynamic_pointer_cast<ExplainConsumer>(consumer)->getStoreResult();
    for (auto & profile_log : remote_profiles)
    {
        ProcessorProfilePtr profile = std::make_shared<ProcessorProfile>();
        profile->processor_name = profile_log.processor_name;
        profile->id = profile_log.id;
        profile->parent_ids = profile_log.parent_ids;
        profile->step_id = profile_log.step_id;
        profile->segment_id = (profile_log.plan_group << 32) >> 48;
        profile->elapsed_us = profile_log.elapsed_us;
        profile->input_wait_elapsed_us = profile_log.input_wait_elapsed_us;
        profile->output_wait_elapsed_us = profile_log.output_wait_elapsed_us;
        profile->input_rows = profile_log.input_rows;
        profile->input_bytes = profile_log.input_bytes;
        profile->output_rows = profile_log.output_rows;
        profile->output_bytes = profile_log.output_bytes;
        segment_profiles[profile->segment_id][profile_log.worker_address].push_back(profile);
    }
}

void ExplainAnalyzeTransform::getProcessorProfiles(ProcessorsSet & processors_set, ProcessorProfiles & profiles, const IProcessor * processor)
{
    auto get_proc_id = [](const IProcessor & proc) -> UInt64 { return reinterpret_cast<std::uintptr_t>(&proc); };

    const auto & inputs = processor->getInputs();
    for (const auto & input : inputs)
    {
        const IProcessor * from = &input.getOutputPort().getProcessor();
        if (processors_set.find(from) == processors_set.end())
        {
            ProcessorProfilePtr child = std::make_shared<ProcessorProfile>();
            child->processor_name = from->getName();

            std::vector<ProcessorId> parents;
            for (const auto & port : from->getOutputs())
            {
                if (!port.isConnected())
                    continue;
                const IProcessor & next = port.getInputPort().getProcessor();

                if (next.getName() == "ExplainAnalyzeTransform")
                    continue;

                parents.push_back(get_proc_id(next));
            }

            child->id = get_proc_id(*from);
            child->parent_ids = std::move(parents);
            child->step_id = from->getStepId();
            child->segment_id = 0;
            child->elapsed_us = from->getElapsedUs();
            child->input_wait_elapsed_us = from->getInputWaitElapsedUs();
            child->output_wait_elapsed_us = from->getOutputWaitElapsedUs();
            child->input_rows = from->getProcessorDataStats().input_rows;
            child->input_bytes = from->getProcessorDataStats().input_bytes;
            child->output_rows = from->getProcessorDataStats().output_rows;
            child->output_bytes = from->getProcessorDataStats().output_bytes;
            child->worker_address = coordinator_address;
            processors_set.insert(from);
            profiles.emplace_back(child);
            getProcessorProfiles(processors_set, profiles, from);
        }
    }
}
}
