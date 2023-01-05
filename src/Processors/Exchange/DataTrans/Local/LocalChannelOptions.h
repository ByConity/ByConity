#pragma once
#include <common/types.h>
namespace DB {

struct LocalChannelOptions{
    size_t queue_size;
    UInt32 max_timeout_ms;
};
}
