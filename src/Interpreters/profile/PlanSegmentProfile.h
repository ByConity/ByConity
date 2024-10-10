#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <Interpreters/ProcessorProfile.h>
#include <Protos/EnumMacros.h>
#include <Protos/enum.pb.h>
#include <Protos/plan_segment_manager.pb.h>
#include <common/types.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
struct RuntimeAttributeDescription;
using AttributeInfoPtr = std::shared_ptr<RuntimeAttributeDescription>;

struct InputProfileMetric
{
    UInt64 id;
    UInt64 input_rows;
    UInt64 input_bytes;
    UInt64 input_wait_sum_elapsed_us = 0;
    UInt64 input_wait_max_elapsed_us = 0;
    UInt64 input_wait_min_elapsed_us{UINT64_MAX};
    void fillFromProto(const Protos::InputProfileMetric & proto);
    void toProto(Protos::InputProfileMetric & proto) const;
};

struct ProfileMetric;
using ProfileMetricPtr = std::shared_ptr<ProfileMetric>;
using ProfileMetrics = std::vector<ProfileMetricPtr>;

struct ProfileMetric
{
    UInt64 id;
    String name; // only use for pipeline profile
    std::vector<UInt64> children_ids;
    UInt32 parallel_size; // only use for pipeline profile
    UInt32 worker_cnt = 0;

    UInt64 sum_elapsed_us;
    UInt64 max_elapsed_us;
    UInt64 min_elapsed_us;

    UInt64 output_rows;
    UInt64 output_bytes;
    UInt64 output_wait_sum_elapsed_us = 0;
    UInt64 output_wait_max_elapsed_us = 0;
    UInt64 output_wait_min_elapsed_us{UINT64_MAX};
    std::unordered_map<UInt64, InputProfileMetric> inputs;

    std::unordered_map<String, AttributeInfoPtr> attributes; // only use for plan profile

    std::unordered_map<String, std::unordered_map<String, AttributeInfoPtr>> address_to_attributes;

    static ProfileMetricPtr fromProto(const Protos::ProfileMetric & proto);
    void toProto(Protos::ProfileMetric & proto);
    static StepProfiles aggregateStepProfileBetweenWorkers(AddressToStepProfile & addr_to_step_profile);
};

struct PlanSegmentProfile;
using PlanSegmentProfilePtr = std::shared_ptr<PlanSegmentProfile>;
using PlanSegmentProfiles = std::vector<PlanSegmentProfilePtr>;
struct PlanSegmentProfile
{
    String query_id;
    UInt64 segment_id;
    bool is_succeed;
    String worker_address;

    UInt64 profile_root_id;
    std::unordered_map<UInt64, ProfileMetricPtr> profiles;

    std::unordered_map<String, AttributeInfoPtr> attributes;

    UInt64 read_rows;
    UInt64 read_bytes;
    UInt64 total_cpu_ms{};
    UInt64 query_duration_ms{};
    UInt64 io_wait_ms{};

    String error_message;

public:
    PlanSegmentProfile() = default;
    explicit PlanSegmentProfile(String query_id_, UInt64 segment_id_) : query_id(query_id_), segment_id(segment_id_)
    {
    }
    static PlanSegmentProfilePtr fromProto(const Protos::PlanSegmentProfileRequest & proto);
    void toProto(Protos::PlanSegmentProfileRequest & proto);
    static PlanSegmentProfilePtr getFromProcessors(const Processors & processors, ContextPtr context);
};

}
