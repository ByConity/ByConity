#include <DataStreams/StreamLocalLimits.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Processors/QueryPlan/PlanSerDerHelper.h>

namespace DB
{

void StreamLocalLimits::serialize(WriteBuffer & buf) const
{
    serializeEnum(mode, buf);
    size_limits.serialize(buf);
    speed_limits.serialize(buf);
    serializeEnum(timeout_overflow_mode, buf);
}

void StreamLocalLimits::deserialize(ReadBuffer & buf)
{
    deserializeEnum(mode, buf);
    size_limits.deserialize(buf);
    speed_limits.deserialize(buf);
    deserializeEnum(timeout_overflow_mode, buf);
}


}
