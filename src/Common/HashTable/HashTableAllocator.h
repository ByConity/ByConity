#pragma once

#include <Common/config.h>
#include <Common/Allocator.h>
#include <Common/HuAllocator.h>


/**
  * We are going to use the entire memory we allocated when resizing a hash
  * table, so it makes sense to pre-fault the pages so that page faults don't
  * interrupt the resize loop. Set the allocator parameter accordingly.
  */
#if USE_HUALLOC
using HashTableAllocator = HuAllocator<true>;
#else
using HashTableAllocator = Allocator<true /* clear_memory */, true /* mmap_populate */>;
#endif

template <size_t initial_bytes = 64>
using HashTableAllocatorWithStackMemory = AllocatorWithStackMemory<HashTableAllocator, initial_bytes>;
