#include <memory>
#include <Interpreters/profile/PlanSegmentProfile.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanSerDerHelper.h>


namespace DB
{
void InputProfileMetric::fillFromProto(const Protos::InputProfileMetric & proto)
{
    id = proto.id();
    input_rows = proto.input_rows();
    input_bytes = proto.input_bytes();
    input_wait_sum_elapsed_us = proto.input_wait_sum_elapsed_us();
    input_wait_max_elapsed_us = proto.input_wait_max_elapsed_us();
    input_wait_min_elapsed_us = proto.input_wait_min_elapsed_us();
}
void InputProfileMetric::toProto(Protos::InputProfileMetric & proto) const
{
    proto.set_id(id);
    proto.set_input_rows(input_rows);
    proto.set_input_bytes(input_bytes);
    proto.set_input_wait_sum_elapsed_us(input_wait_sum_elapsed_us);
    proto.set_input_wait_max_elapsed_us(input_wait_max_elapsed_us);
    proto.set_input_wait_min_elapsed_us(input_wait_min_elapsed_us);
}

ProfileMetricPtr ProfileMetric::fromProto(const Protos::ProfileMetric & proto)
{
    ProfileMetricPtr profile = std::make_shared<ProfileMetric>();
    profile->id = proto.id();
    profile->name = proto.name();

    if (!proto.children_ids().empty())
    {
        for (const auto & id : proto.children_ids())
            profile->children_ids.emplace_back(id);
    }
    profile->parallel_size = proto.parallel_size();
    profile->min_elapsed_us = proto.min_elapsed_us();
    profile->sum_elapsed_us = proto.sum_elapsed_us();
    profile->max_elapsed_us = proto.max_elapsed_us();
    profile->output_rows = proto.output_rows();
    profile->output_bytes = proto.output_bytes();
    profile->output_wait_sum_elapsed_us = proto.output_wait_sum_elapsed_us();
    profile->output_wait_max_elapsed_us = proto.output_wait_max_elapsed_us();
    profile->output_wait_min_elapsed_us = proto.output_wait_min_elapsed_us();

    for (const auto & proto_input : proto.inputs())
    {
        InputProfileMetric input_profile;
        input_profile.fillFromProto(proto_input);
        profile->inputs.emplace(input_profile.id, input_profile);
    }

    for (const auto & [attribute_type, attribute] : proto.attributes())
    {
        AttributeInfoPtr info = std::make_shared<RuntimeAttributeDescription>();
        info->fillFromProto(attribute);
        profile->attributes.emplace(attribute_type, info);
    }
    return profile;
}

void ProfileMetric::toProto(Protos::ProfileMetric & proto)
{
    proto.set_id(id);
    proto.set_name(name);
    for (auto & child_id : children_ids)
        proto.add_children_ids(child_id);
    proto.set_parallel_size(parallel_size);

    proto.set_sum_elapsed_us(sum_elapsed_us);
    proto.set_min_elapsed_us(min_elapsed_us);
    proto.set_max_elapsed_us(max_elapsed_us);

    proto.set_output_rows(output_rows);
    proto.set_output_bytes(output_bytes);
    proto.set_output_wait_sum_elapsed_us(output_wait_sum_elapsed_us);
    proto.set_output_wait_max_elapsed_us(output_wait_max_elapsed_us);
    proto.set_output_wait_min_elapsed_us(output_wait_min_elapsed_us);

    for (auto & input : inputs)
        input.second.toProto(*proto.add_inputs());

    for (auto & att : attributes)
    {
        auto * att_proto = &(*proto.mutable_attributes())[att.first];
        att.second->toProto(*att_proto);
    }
}

StepProfiles ProfileMetric::aggregateStepProfileBetweenWorkers(AddressToStepProfile & addr_to_step_profile)
{
    StepProfiles res;
    for (auto & [address, stepid_to_profile] : addr_to_step_profile)
    {
        for (auto & [step_id, step_profile] : stepid_to_profile)
        {
            if (!res.contains(step_id))
            {
                step_profile->worker_cnt = 1;
                if (!step_profile->attributes.empty())
                    step_profile->address_to_attributes[address] = step_profile->attributes;
                res[step_id] = step_profile;
            }
            else
            {
                auto & profile_ptr = res.at(step_id);
                profile_ptr->max_elapsed_us = std::max(profile_ptr->max_elapsed_us, step_profile->sum_elapsed_us);
                profile_ptr->min_elapsed_us = std::min(profile_ptr->min_elapsed_us, step_profile->sum_elapsed_us);
                profile_ptr->sum_elapsed_us += step_profile->sum_elapsed_us;
                profile_ptr->worker_cnt++;
                profile_ptr->output_wait_max_elapsed_us
                    = std::max(profile_ptr->output_wait_max_elapsed_us, step_profile->output_wait_max_elapsed_us);
                profile_ptr->output_wait_min_elapsed_us
                    = std::min(profile_ptr->output_wait_min_elapsed_us, step_profile->output_wait_max_elapsed_us);
                profile_ptr->output_wait_sum_elapsed_us += step_profile->output_wait_max_elapsed_us;
                profile_ptr->output_rows += step_profile->output_rows;
                profile_ptr->output_bytes += step_profile->output_bytes;

                for (auto & [id, input_profile] : step_profile->inputs)
                {
                    profile_ptr->inputs[id].input_wait_sum_elapsed_us += input_profile.input_wait_max_elapsed_us;
                    profile_ptr->inputs[id].input_wait_max_elapsed_us
                        = std::max(profile_ptr->inputs[id].input_wait_max_elapsed_us, input_profile.input_wait_max_elapsed_us);
                    profile_ptr->inputs[id].input_wait_min_elapsed_us
                        = std::min(profile_ptr->inputs[id].input_wait_min_elapsed_us, input_profile.input_wait_max_elapsed_us);
                    profile_ptr->inputs[id].input_rows += input_profile.input_rows;
                    profile_ptr->inputs[id].input_bytes += input_profile.input_bytes;
                }
                if (!step_profile->attributes.empty())
                    profile_ptr->address_to_attributes[address] = step_profile->attributes;
            }
        }
    }
    return res;
}

PlanSegmentProfilePtr PlanSegmentProfile::fromProto(const Protos::PlanSegmentProfileRequest & proto)
{
    PlanSegmentProfilePtr segment_profile = std::make_shared<PlanSegmentProfile>();
    segment_profile->query_id = proto.query_id();
    segment_profile->segment_id = proto.segment_id();
    segment_profile->is_succeed = proto.is_succeed();
    segment_profile->worker_address = proto.worker_address();
    if (proto.has_profile_root_id())
        segment_profile->profile_root_id = proto.profile_root_id();

    for (const auto & [profile_id, profile_proto] : proto.profiles())
    {
        auto profile = ProfileMetric::fromProto(profile_proto);
        segment_profile->profiles.emplace(profile_id, profile);
    }
    if (proto.has_read_rows())
        segment_profile->read_rows = proto.read_rows();
    if (proto.has_read_bytes())
        segment_profile->read_bytes = proto.read_bytes();
    if (proto.has_total_cpu_ms())
        segment_profile->total_cpu_ms = proto.total_cpu_ms();
    if (proto.has_query_duration_ms())
        segment_profile->query_duration_ms = proto.query_duration_ms();
    if (proto.has_io_wait_ms())
        segment_profile->io_wait_ms = proto.io_wait_ms();
    if (proto.has_error_message())
        segment_profile->error_message = proto.error_message();
    return segment_profile;
}

void PlanSegmentProfile::toProto(Protos::PlanSegmentProfileRequest & proto)
{
    proto.set_query_id(query_id);
    proto.set_segment_id(segment_id);
    proto.set_is_succeed(is_succeed);
    proto.set_worker_address(worker_address);

    proto.set_profile_root_id(profile_root_id);

    for (auto & p : profiles)
    {
        auto * profile_proto = &(*proto.mutable_profiles())[p.first];
        p.second->toProto(*profile_proto);
    }
    proto.set_read_rows(read_rows);
    proto.set_read_bytes(read_bytes);
    proto.set_total_cpu_ms(total_cpu_ms);
    proto.set_query_duration_ms(query_duration_ms);
    proto.set_io_wait_ms(io_wait_ms);
    proto.set_error_message(error_message);
}

}
