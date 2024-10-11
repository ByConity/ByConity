#pragma once

#include <common/types.h>

namespace DB
{

struct ResourceRequest
{
    UInt32 segment_id;
    UInt32 parallel_index;
    String worker_id;
    UInt32 v_cpu{1};
    UInt32 mem{0};
    UInt32 epoch{0};
};

} // namespace DB
