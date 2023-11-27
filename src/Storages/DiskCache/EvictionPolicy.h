#pragma once

#include <Storages/DiskCache/Region.h>
#include <Storages/DiskCache/Types.h>

namespace DB::HybridCache
{
// Abstract base class of an eviction policy.
class EvictionPolicy
{
public:
    virtual ~EvictionPolicy() = default;

    // Adds a new region for tracking.
    virtual void track(const Region & region) = 0;

    // Touches this region.
    virtual void touch(RegionId id) = 0;

    // Evicts a region and stop tracking.
    virtual RegionId evict() = 0;

    // Resets policy to the internal stae.
    virtual void reset() = 0;

    // Gets memory used by the policy.
    virtual size_t memorySize() const = 0;
};
}
