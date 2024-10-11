#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/SystemLog.h>
#include <Processors/IProcessor.h>
#include <Protos/plan_node_utils.pb.h>
#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>

#include <chrono>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{

struct ProcessorProfile;
using ProcessorProfilePtr = std::shared_ptr<ProcessorProfile>;
using ProcessorProfiles = std::vector<ProcessorProfilePtr>;
using QueryProcessorProfiles = std::map<size_t, std::vector<ProcessorProfilePtr>>; // plan_segment_id -> stream_level_operator_profile
using ProcessorId = UInt64;

struct ProcessorProfile
{
    ProcessorId id{};
    String processor_name;
    std::vector<ProcessorId> parent_ids;

    int64_t step_id = -1;
    UInt64 segment_id = 0;
    String worker_address;

    /// Milliseconds spend in IProcessor::work()
    UInt64 elapsed_us{};
    /// IProcessor::NeedData
    UInt64 input_wait_elapsed_us{};
    /// IProcessor::PortFull
    UInt64 output_wait_elapsed_us{};

    size_t input_rows{};
    size_t input_bytes{};
    size_t output_rows{};
    size_t output_bytes{};

public:
    ProcessorProfile() = default;
    explicit ProcessorProfile(String name): processor_name(name) {}
    explicit ProcessorProfile(const IProcessor * processor);
};

struct GroupedProcessorProfile;
using GroupedProcessorProfilePtr = std::shared_ptr<GroupedProcessorProfile>;
using GroupedProcessorProfiles = std::vector<GroupedProcessorProfilePtr>;
using SegIdAndAddrToPipelineProfile = std::unordered_map<size_t, std::unordered_map<String, GroupedProcessorProfilePtr>>;
struct ProfileMetric;
using ProfileMetricPtr = std::shared_ptr<ProfileMetric>;
using ProfileMetrics = std::vector<ProfileMetricPtr>;
using IdToProfileMetrics = std::unordered_map<size_t, ProfileMetrics>;
using StepProfiles = std::unordered_map<size_t, ProfileMetricPtr>; // step_id -> aggregated profile
using AddressToStepProfile = std::unordered_map<String, std::unordered_map<size_t, ProfileMetricPtr>>; // step_id -> aggregated profile

struct GroupedProcessorProfile
{
    ProcessorId id{};
    String processor_name;

    int64_t step_id = -1;
    UInt64 sum_grouped_elapsed_us{};
    UInt64 sum_grouped_input_wait_elapsed_us{};
    UInt64 sum_grouped_output_wait_elapsed_us{};

    UInt64 grouped_input_rows{};
    UInt64 grouped_input_bytes{};
    UInt64 grouped_output_rows{};
    UInt64 grouped_output_bytes{};
    size_t parallel_size = 0;

    std::unordered_set<ProcessorId> processor_ids;
    bool visited = false;
    std::unordered_map<String, GroupedProcessorProfilePtr> parents; // Be careful to avoid circular dependencies between parents and children
    std::unordered_set<int64_t> parent_step_ids;
    std::vector<GroupedProcessorProfilePtr> children;

    UInt64 worker_cnt = 1;
    UInt64 max_grouped_elapsed_us{};
    UInt64 min_grouped_elapsed_us{UINT64_MAX};
    UInt64 max_grouped_input_wait_elapsed_us{};
    UInt64 min_grouped_input_wait_elapsed_us{UINT64_MAX};
    UInt64 max_grouped_output_wait_elapsed_us{};
    UInt64 min_grouped_output_wait_elapsed_us{UINT64_MAX};
public:
    GroupedProcessorProfile() = default;
    explicit GroupedProcessorProfile(ProcessorId id_, String name_) : id(id_), processor_name(name_) {}
    static GroupedProcessorProfilePtr getGroupedProfiles(ProcessorProfiles & profiles);
    static std::set<GroupedProcessorProfilePtr> fillChildren(GroupedProcessorProfilePtr & input_processor, std::set<ProcessorId> & visited);
    static GroupedProcessorProfilePtr getOutputRoot(GroupedProcessorProfilePtr & input_root);
    void add(ProcessorId processor_id, const ProcessorProfilePtr & profile);

    static SegIdAndAddrToPipelineProfile aggregatePipelineProfileBetweenWorkers(SegIdAndAddrToPipelineProfile & worker_grouped_profiles);
    static UInt128 getPipelineProfilehash(GroupedProcessorProfilePtr & node);
    void addProfileRecursively(GroupedProcessorProfilePtr & profile);

    static std::unordered_map<UInt64, ProfileMetricPtr> getProfileMetricsFromOutputRoot(GroupedProcessorProfilePtr & output_root);
    static GroupedProcessorProfilePtr
    getGroupedProfileFromMetrics(std::unordered_map<UInt64, ProfileMetricPtr> & profile_map, UInt64 root_id);

    static StepProfiles aggregateOperatorProfileToStepLevel(GroupedProcessorProfilePtr & processor_profile_root);
    Poco::JSON::Object::Ptr getJsonProfiles();
};

}
