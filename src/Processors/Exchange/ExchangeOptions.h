#pragma once

#include "common/types.h"
namespace DB {

struct ExchangeOptions
{
    UInt32 exhcange_timeout_ms;
    UInt64 send_threshold_in_bytes;
    UInt32 send_threshold_in_row_num;
};

}

