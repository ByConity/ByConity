#pragma once
#include <cstdint>

struct LSNStatus
{
    uint64_t committed_lsn;
    uint64_t updated_lsn;
    uint64_t max_lsn;
    uint64_t count;
};
