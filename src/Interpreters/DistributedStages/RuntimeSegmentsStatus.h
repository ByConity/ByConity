#pragma once

#include <Protos/plan_segment_manager.pb.h>
#include <common/types.h>


namespace DB
{
namespace Protos
{
    class RuntimeSegmentsMetrics;
}

struct RuntimeSegmentsMetrics
{
    UInt64 cpu_micros;
    Protos::Progress final_progress;

    RuntimeSegmentsMetrics() : cpu_micros(0)
    {
    }

    explicit RuntimeSegmentsMetrics(const Protos::RuntimeSegmentsMetrics & metrics_)
    {
        cpu_micros = metrics_.cpu_micros();
        if (metrics_.has_progress())
        {
            final_progress = metrics_.progress();
        }
    }

    void setProtos(Protos::RuntimeSegmentsMetrics & metrics_) const
    {
        metrics_.set_cpu_micros(cpu_micros);
        *metrics_.mutable_progress() = final_progress;
    }
};

struct RuntimeSegmentStatus
{
    String query_id;
    int32_t segment_id{0};
    size_t parallel_index{0};
    int32_t attempt_id{0};
    bool is_succeed{true};
    bool is_cancelled{false};
    RuntimeSegmentsMetrics metrics;
    String message;
    int32_t code{0};
};
}
