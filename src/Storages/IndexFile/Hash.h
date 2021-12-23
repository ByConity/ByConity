#pragma once

#include <stddef.h>
#include <stdint.h>

namespace DB::IndexFile
{
// Simple hash function used for internal data structures
// FIXME replace with 64-bits xxhash
uint32_t Hash(const char * data, size_t n, uint32_t seed);

}
