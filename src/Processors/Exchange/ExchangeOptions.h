#pragma once

#include "common/types.h"
namespace DB {

struct ExchangeOptions
{
    UInt32 exhcange_timeout_ms;
    UInt64 send_threshold_in_bytes;
    UInt64 send_threshold_in_row_num;
    bool force_remote_mode = false;
    bool need_send_plan_segment_status = true;
    bool force_use_buffer = false;
};

}

