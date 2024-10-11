#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <Interpreters/ProcessorProfile.h>
#include <Interpreters/profile/PlanSegmentProfile.h>
#include "common/types.h"
#include <common/types.h>

namespace DB
{

ProcessorProfile::ProcessorProfile(const IProcessor * processor)
{
    processor_name = processor->getName();

    auto get_proc_id = [](const IProcessor & proc) -> UInt64 { return reinterpret_cast<std::uintptr_t>(&proc); };

    std::vector<ProcessorId> parents;
    for (const auto & port : processor->getOutputs())
    {
        if (!port.isConnected())
            continue;
        const IProcessor & next = port.getInputPort().getProcessor();
        parents.push_back(get_proc_id(next));
    }

    id = get_proc_id(*processor);
    parent_ids = std::move(parents);
    step_id = processor->getStepId();
    elapsed_us = processor->getElapsedUs();
    input_wait_elapsed_us = processor->getInputWaitElapsedUs();
    output_wait_elapsed_us = processor->getOutputWaitElapsedUs();
    input_rows = processor->getProcessorDataStats().input_rows;
    input_bytes = processor->getProcessorDataStats().input_bytes;
    output_rows = processor->getProcessorDataStats().output_rows;
    output_bytes = processor->getProcessorDataStats().output_bytes;
}

GroupedProcessorProfilePtr GroupedProcessorProfile::getGroupedProfiles(ProcessorProfiles & profiles)
{
    /// input processor id -> target processor id
    std::unordered_map<ProcessorId, std::vector<ProcessorId>> dag;
    std::unordered_map<ProcessorId, ProcessorProfilePtr> profile_map;
    std::unordered_set<ProcessorId> non_root_set;

    for (auto & profile : profiles)
    {
        for (auto parents_id : profile->parent_ids)
        {
            dag[profile->id].push_back(parents_id);
            non_root_set.insert(parents_id);
        }
        profile_map[profile->id] = profile;
    }

    std::vector<ProcessorId> roots;
    /// get root input processors
    for (auto & profile : profiles)
        if (!non_root_set.contains(profile->id))
            roots.emplace_back(profile->id);

    /// Build profile group tree
    std::vector<GroupedProcessorProfilePtr> groups;
    std::unordered_map<ProcessorId, GroupedProcessorProfilePtr> profile_to_group;
    auto root = std::make_shared<GroupedProcessorProfile>(0, "input_root");
    root->visited = true;
    groups.emplace_back(root);

    for (const auto & id : roots)
    {
        auto & group_parents = root->parents;
        auto & root_profile = profile_map[id];
        auto parent_processor_name = root_profile->processor_name + (root_profile->step_id > 0 ? std::to_string(root_profile->step_id) : "");
        if (!group_parents.contains(parent_processor_name))
        {
            auto parent = std::make_shared<GroupedProcessorProfile>(groups.size(), root_profile->processor_name);
            group_parents.emplace(parent_processor_name, parent);
            groups.emplace_back(parent);
        }
        group_parents[parent_processor_name]->add(id, root_profile);
        profile_to_group.emplace(id, group_parents[parent_processor_name]);
    }

    for (size_t i = 1; i < groups.size(); i++)
    {
        auto group = groups[i];
        if (group->visited)
            continue;
        group->visited = true;

        /// Build parent group, then add it to children.
        auto & group_parents = group->parents;
        for (const auto & processor_id : group->processor_ids)
        {
            for (auto & parent_processor_id : dag[processor_id])
            {
                const auto & parent_processor = profile_map[parent_processor_id];
                if (!parent_processor)
                    continue;
                auto parent_processor_name = parent_processor->processor_name + (parent_processor->step_id > 0 ? std::to_string(parent_processor->step_id) : "");
                if (!group_parents.contains(parent_processor_name))
                {
                    if (profile_to_group.count(parent_processor_id))
                        group_parents.emplace(parent_processor_name, profile_to_group[parent_processor_id]);
                    else
                    {
                        auto child = std::make_shared<GroupedProcessorProfile>(groups.size(), parent_processor->processor_name);
                        group_parents.emplace(parent_processor_name, child);
                        groups.emplace_back(child);
                    }
                }
                group_parents[parent_processor_name]->add(parent_processor_id, parent_processor);
                profile_to_group.emplace(parent_processor_id, group_parents[parent_processor_name]);
            }
        }
    }

    return root;
}

void GroupedProcessorProfile::add(ProcessorId processor_id, const ProcessorProfilePtr & profile)
{
    if (processor_ids.count(processor_id))
        return;

    if (profile->step_id != -1)
        step_id = profile->step_id;
    processor_ids.emplace(processor_id);
    parallel_size += 1;
    sum_grouped_elapsed_us += profile->elapsed_us;
    sum_grouped_input_wait_elapsed_us += profile->input_wait_elapsed_us;
    sum_grouped_output_wait_elapsed_us += profile->output_wait_elapsed_us;
    grouped_input_rows +=  profile->input_rows;
    grouped_input_bytes +=  profile->input_bytes;
    grouped_output_rows +=  profile->output_rows;
    grouped_output_bytes +=  profile->output_bytes;

    worker_cnt = 1;
    max_grouped_elapsed_us = std::max(max_grouped_elapsed_us, profile->elapsed_us);
    min_grouped_elapsed_us = std::min(min_grouped_elapsed_us, profile->elapsed_us);
    max_grouped_input_wait_elapsed_us = std::max(max_grouped_input_wait_elapsed_us, profile->input_wait_elapsed_us);
    min_grouped_input_wait_elapsed_us = std::min(min_grouped_input_wait_elapsed_us, profile->input_wait_elapsed_us);
    max_grouped_output_wait_elapsed_us = std::max(max_grouped_output_wait_elapsed_us, profile->output_wait_elapsed_us);
    min_grouped_output_wait_elapsed_us = std::min(min_grouped_output_wait_elapsed_us, profile->output_wait_elapsed_us);
}

std::set<GroupedProcessorProfilePtr> GroupedProcessorProfile::fillChildren(GroupedProcessorProfilePtr & input_processor, std::set<ProcessorId> & visited)
{
    if (input_processor->parents.empty())
        return {input_processor};
    visited.emplace(input_processor->id);
    std::set<GroupedProcessorProfilePtr> outputs;
    for (auto & item : input_processor->parents)
    {
        if (input_processor->processor_name != "input_root")
            item.second->children.emplace_back(input_processor);
        if (visited.contains(item.second->id))
            continue;
        auto roots = fillChildren(item.second, visited);
        for (const auto & root : roots)
            outputs.insert(root);
        input_processor->parent_step_ids.emplace(item.second->step_id);
    }
    input_processor->parents.clear();
    return outputs;
}

GroupedProcessorProfilePtr GroupedProcessorProfile::getOutputRoot(GroupedProcessorProfilePtr & input_root)
{
    if (input_root->processor_name == "output_root")
        return input_root;
    std::set<ProcessorId> visited;
    std::set<GroupedProcessorProfilePtr> outputs;
    outputs = fillChildren(input_root, visited);
    auto output_root = std::make_shared<GroupedProcessorProfile>(0, "output_root");
    output_root->children = {outputs.begin(), outputs.end()};
    return output_root;
}

UInt128 GroupedProcessorProfile::getPipelineProfilehash(GroupedProcessorProfilePtr & node)
{
    SipHash hash;
    UInt128 key{};
    GroupedProcessorProfiles profiles;
    profiles.push_back(node);
    while (!profiles.empty())
    {
        auto profile = profiles.back();
        profiles.pop_back();
        if (profile->processor_name.starts_with("MergeTree"))
            hash.update("MergeTree");
        else if (profile->processor_name.starts_with("MultiPathReceiver"))
            hash.update("MultiPathReceiver");
        else
            hash.update(profile->processor_name);
        for (auto & child : profile->children)
            profiles.push_back(child);
    }
    hash.get128(key);
    return key;
}

SegIdAndAddrToPipelineProfile
GroupedProcessorProfile::aggregatePipelineProfileBetweenWorkers(SegIdAndAddrToPipelineProfile & worker_grouped_profiles)
{
    SegIdAndAddrToPipelineProfile res;
    for (auto [segment, woker_profile_map] : worker_grouped_profiles)
    {
        std::unordered_map<UInt128, std::unordered_map<String, GroupedProcessorProfilePtr>> hash_to_profiles;

        for (auto & [worker_ip, profile] : woker_profile_map)
        {
            auto pipeline_hash = getPipelineProfilehash(profile);
            hash_to_profiles[pipeline_hash].emplace(worker_ip, profile);
        }

        for (auto [hash, woker_profile] : hash_to_profiles)
        {
            String workers_ip_list_str = "[";
            GroupedProcessorProfilePtr aggregate_profile = nullptr;
            for (auto & [worker_ip, profile] : woker_profile)
            {
                if (!aggregate_profile)
                {
                    workers_ip_list_str = workers_ip_list_str + worker_ip;
                    aggregate_profile = profile;
                    continue;
                }
                workers_ip_list_str = workers_ip_list_str + "," + worker_ip;
                aggregate_profile->addProfileRecursively(profile);
            }
            workers_ip_list_str = workers_ip_list_str + "]";
            res[segment][workers_ip_list_str] = aggregate_profile;
        }
    }
    return res;
}

void GroupedProcessorProfile::addProfileRecursively(GroupedProcessorProfilePtr & profile)
{
    if (!profile)
        return;

    parallel_size += profile->parallel_size;
    worker_cnt++;
    sum_grouped_elapsed_us += profile->sum_grouped_elapsed_us;
    max_grouped_elapsed_us = std::max(max_grouped_elapsed_us, profile->max_grouped_elapsed_us);
    min_grouped_elapsed_us = std::min(min_grouped_elapsed_us, profile->min_grouped_elapsed_us);
    sum_grouped_input_wait_elapsed_us += profile->sum_grouped_input_wait_elapsed_us;
    max_grouped_input_wait_elapsed_us = std::max(max_grouped_input_wait_elapsed_us, profile->max_grouped_input_wait_elapsed_us);
    min_grouped_input_wait_elapsed_us = std::min(min_grouped_input_wait_elapsed_us, profile->min_grouped_input_wait_elapsed_us);
    sum_grouped_output_wait_elapsed_us += profile->sum_grouped_output_wait_elapsed_us;
    max_grouped_output_wait_elapsed_us = std::max(max_grouped_output_wait_elapsed_us, profile->max_grouped_output_wait_elapsed_us);
    min_grouped_output_wait_elapsed_us = std::min(min_grouped_output_wait_elapsed_us, profile->min_grouped_output_wait_elapsed_us);
    grouped_input_rows +=  profile->grouped_input_rows;
    grouped_input_bytes +=  profile->grouped_input_bytes;
    grouped_output_rows +=  profile->grouped_output_rows;
    grouped_output_bytes +=  profile->grouped_output_bytes;

    for (size_t i = 0; i < children.size(); i++)
    {
        if (i < profile->children.size() && profile->children[i])
            children[i]->addProfileRecursively(profile->children[i]);
    }
}

Poco::JSON::Object::Ptr GroupedProcessorProfile::getJsonProfiles()
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(true);
    json->set("ProcessorName", processor_name);
    json->set("StepId", step_id);
    json->set("ParallelSize", parallel_size);
    json->set("ElapsedUs", UInt64(sum_grouped_elapsed_us / parallel_size));
    json->set("MaxElapsedUs", max_grouped_elapsed_us);
    json->set("MinElapsedUs", min_grouped_elapsed_us);
    json->set("InputWaitElapsedUs", UInt64(sum_grouped_input_wait_elapsed_us / parallel_size));
    json->set("MaxInputWaitElapsedUs", max_grouped_input_wait_elapsed_us);
    json->set("MinInputWaitElapsedUs", min_grouped_input_wait_elapsed_us);
    json->set("OutputWaitElapsedUs", UInt64(sum_grouped_output_wait_elapsed_us / parallel_size));
    json->set("MaxOutputWaitElapsedUs", max_grouped_output_wait_elapsed_us);
    json->set("MinOutputWaitElapsedUs", min_grouped_output_wait_elapsed_us);
    json->set("InputRows", grouped_input_rows);
    json->set("InputBytes", grouped_input_bytes);
    json->set("OutputRows", grouped_output_rows);
    json->set("OutputBytes", grouped_output_bytes);
    Poco::JSON::Array inputs;
    for (auto & child : children)
        inputs.add(child->getJsonProfiles());

    if (!children.empty())
        json->set("Inputs", inputs);
    return json;
}

std::unordered_map<UInt64, ProfileMetricPtr>
GroupedProcessorProfile::getProfileMetricsFromOutputRoot(GroupedProcessorProfilePtr & output_root)
{
    if (output_root->processor_name == "input_root")
        output_root = GroupedProcessorProfile::getOutputRoot(output_root);
    std::unordered_map<UInt64, ProfileMetricPtr> res;
    GroupedProcessorProfiles grouped_profiles;
    grouped_profiles.push_back(output_root);
    while (!grouped_profiles.empty())
    {
        auto node = grouped_profiles.back();
        grouped_profiles.pop_back();
        ProfileMetricPtr profile = std::make_shared<ProfileMetric>();
        profile->id = node->id;
        profile->name = node->processor_name;
        profile->parallel_size = node->parallel_size;
        for (auto & child : node->children)
        {
            profile->children_ids.emplace_back(child->id);
            grouped_profiles.push_back(child);
        }

        profile->sum_elapsed_us = node->sum_grouped_elapsed_us;
        profile->max_elapsed_us = node->max_grouped_elapsed_us;
        profile->min_elapsed_us = node->min_grouped_elapsed_us;
        profile->output_rows = node->grouped_output_rows;
        profile->output_bytes = node->grouped_output_bytes;
        profile->output_wait_sum_elapsed_us = node->sum_grouped_output_wait_elapsed_us;
        profile->output_wait_max_elapsed_us = node->max_grouped_output_wait_elapsed_us;
        profile->output_wait_min_elapsed_us = node->min_grouped_output_wait_elapsed_us;
        InputProfileMetric input;
        input.id = 0;
        input.input_rows = node->grouped_input_rows;
        input.input_bytes = node->grouped_input_bytes;
        input.input_wait_sum_elapsed_us = node->sum_grouped_input_wait_elapsed_us;
        input.input_wait_max_elapsed_us = node->max_grouped_input_wait_elapsed_us;
        input.input_wait_min_elapsed_us = node->min_grouped_input_wait_elapsed_us;
        profile->inputs.emplace(0, input);

        res[profile->id] = profile;
    }
    return res;
}

GroupedProcessorProfilePtr
GroupedProcessorProfile::getGroupedProfileFromMetrics(std::unordered_map<UInt64, ProfileMetricPtr> & profile_map, UInt64 root_id)
{
    if (!profile_map.contains(root_id))
        return nullptr;

    auto profile = profile_map.at(root_id);
    GroupedProcessorProfilePtr node = std::make_shared<GroupedProcessorProfile>();
    node->id = root_id;
    node->processor_name = profile->name;
    node->parallel_size = profile->parallel_size;
    node->grouped_output_rows = profile->output_rows;
    node->grouped_output_bytes = profile->output_bytes;
    node->sum_grouped_elapsed_us = profile->sum_elapsed_us;
    node->max_grouped_elapsed_us = profile->max_elapsed_us;
    node->min_grouped_elapsed_us = profile->min_elapsed_us;

    node->sum_grouped_output_wait_elapsed_us = profile->output_wait_sum_elapsed_us;
    node->max_grouped_output_wait_elapsed_us = profile->output_wait_max_elapsed_us;
    node->min_grouped_output_wait_elapsed_us = profile->output_wait_min_elapsed_us;
    if (!profile->inputs.empty())
    {
        auto & input_profile = profile->inputs[0];
        node->id = input_profile.id;
        node->grouped_input_rows = input_profile.input_rows;
        node->grouped_input_bytes = input_profile.input_bytes;
        node->sum_grouped_input_wait_elapsed_us = input_profile.input_wait_sum_elapsed_us;
        node->max_grouped_input_wait_elapsed_us = input_profile.input_wait_max_elapsed_us;
        node->min_grouped_input_wait_elapsed_us = input_profile.input_wait_min_elapsed_us;
    }

    node->worker_cnt = 1;

    for (auto & child_id : profile->children_ids)
    {
        if (profile_map.contains(child_id))
        {
            auto child = getGroupedProfileFromMetrics(profile_map, child_id);
            // child->parents.emplace(node->processor_name, node);
            node->children.emplace_back(child);
        }
    }
    return node;
}


StepProfiles GroupedProcessorProfile::aggregateOperatorProfileToStepLevel(GroupedProcessorProfilePtr & processor_profile_root)
{
    if (processor_profile_root->processor_name == "input_root")
        processor_profile_root = GroupedProcessorProfile::getOutputRoot(processor_profile_root);

    StepProfiles res;

    struct ProfilesList
    {
        /// input step_id -> processor profile
        std::unordered_map<int64_t, GroupedProcessorProfilePtr> input_profiles;
        std::vector<GroupedProcessorProfilePtr> output_profiles;
        std::unordered_map<size_t, GroupedProcessorProfiles> profiles_at_each_level;
    };

    /// step_id -> map<level, ProfilesList>
    std::unordered_map<Int64, ProfilesList> step_processor_profiles_at_each_level;

    size_t level = 0;
    std::queue<GroupedProcessorProfilePtr> q;
    std::unordered_set<ProcessorId> id_set;
    q.push(processor_profile_root);
    id_set.emplace(processor_profile_root->id);
    while (!q.empty())
    {
        size_t size = q.size();
        for (size_t i = 0; i < size; i++)
        {
            auto processor_profile = q.front();
            q.pop();
            auto & current_step_id = processor_profile->step_id;
            auto & inputs = processor_profile->children;
            auto & outputs = processor_profile->parent_step_ids;

            if (current_step_id == -1 && !outputs.empty() && processor_profile->processor_name != "output_root")
                current_step_id = *outputs.begin();

            step_processor_profiles_at_each_level[current_step_id].profiles_at_each_level[level].push_back(processor_profile);

            if (outputs.empty())
                step_processor_profiles_at_each_level[current_step_id].output_profiles.push_back(processor_profile);

            if (inputs.empty())
                step_processor_profiles_at_each_level[current_step_id].input_profiles[current_step_id] = processor_profile;

            for (auto & input_profile : inputs)
            {
                if (input_profile->step_id != -1 && current_step_id != input_profile->step_id)
                {
                    step_processor_profiles_at_each_level[current_step_id].input_profiles[input_profile->step_id] = processor_profile;
                    step_processor_profiles_at_each_level[input_profile->step_id].output_profiles.push_back(input_profile);
                }
                if (!id_set.contains(input_profile->id))
                {
                    q.push(input_profile);
                    id_set.emplace(input_profile->id);
                }
            }
        }
        level++;
    }

    for (auto & [step_id, profiles_list] : step_processor_profiles_at_each_level)
    {
        if (step_id == -1)
            continue;
        auto step_profile = std::make_shared<ProfileMetric>();

        for (auto & output_profile : profiles_list.output_profiles)
        {
            step_profile->output_bytes += output_profile->grouped_output_bytes;
            step_profile->output_rows += output_profile->grouped_output_rows;
            step_profile->output_wait_sum_elapsed_us += output_profile->max_grouped_output_wait_elapsed_us;
            step_profile->output_wait_max_elapsed_us
                = std::max(step_profile->output_wait_max_elapsed_us, output_profile->max_grouped_output_wait_elapsed_us);
            step_profile->output_wait_min_elapsed_us
                = std::min(step_profile->output_wait_min_elapsed_us, output_profile->min_grouped_output_wait_elapsed_us);
        }

        for (auto & [input_step_id, input_profile] : profiles_list.input_profiles)
        {
            step_profile->inputs[input_step_id].id = input_step_id;
            step_profile->inputs[input_step_id].input_rows = input_profile->grouped_input_rows;
            step_profile->inputs[input_step_id].input_bytes = input_profile->grouped_input_bytes;
            step_profile->inputs[input_step_id].input_wait_sum_elapsed_us += input_profile->max_grouped_output_wait_elapsed_us;
            step_profile->inputs[input_step_id].input_wait_max_elapsed_us = std::max(
                step_profile->inputs[input_step_id].input_wait_max_elapsed_us, input_profile->max_grouped_output_wait_elapsed_us);
            step_profile->inputs[input_step_id].input_wait_min_elapsed_us = std::min(
                step_profile->inputs[input_step_id].input_wait_min_elapsed_us, input_profile->min_grouped_output_wait_elapsed_us);
        }

        for (auto & [_, level_profiles] : profiles_list.profiles_at_each_level)
        {
            for (auto & profile : level_profiles)
                step_profile->sum_elapsed_us += profile->max_grouped_elapsed_us;
        }
        step_profile->id = step_id;
        res[step_id] = step_profile;
    }
    return res;
}

}
