#pragma once

#include <Common/Logger.h>
#include <chrono>

#include <Poco/Logger.h>
#include <folly/fibers/TimedMutex.h>

#include <Storages/DiskCache/EvictionPolicy.h>
#include <Storages/DiskCache/Types.h>
#include <common/chrono_io.h>

namespace DB::HybridCache
{

using folly::fibers::TimedMutex;

namespace detail
{
    struct Node
    {
        const RegionId rid{};
        const std::chrono::seconds track_time{};

        std::chrono::seconds secondsSinceTracking() const { return getSteadyClockSeconds() - track_time; }
    };
}

class FifoPolicy final : public EvictionPolicy
{
public:
    FifoPolicy();
    FifoPolicy(const FifoPolicy &) = delete;
    FifoPolicy & operator=(const FifoPolicy &) = delete;
    ~FifoPolicy() override = default;

    void touch(RegionId /* rid */) override { }

    void track(const Region & region) override;

    RegionId evict() override;

    void reset() override;

    size_t memorySize() const override
    {
        std::lock_guard<TimedMutex> guard{mutex};
        return sizeof(*this) + sizeof(detail::Node) * queue.size();
    }

private:
    LoggerPtr log = getLogger("FifoPolicy");

    std::deque<detail::Node> queue;
    mutable TimedMutex mutex;
};

class SegmentedFifoPolicy final : public EvictionPolicy
{
public:
    explicit SegmentedFifoPolicy(std::vector<unsigned int> segment_ratio);
    SegmentedFifoPolicy(const SegmentedFifoPolicy &) = delete;
    SegmentedFifoPolicy & operator=(const SegmentedFifoPolicy &) = delete;
    ~SegmentedFifoPolicy() override = default;

    void touch(RegionId /* rid */) override { }

    void track(const Region & region) override;

    RegionId evict() override;

    void reset() override;

    size_t memorySize() const override;

private:
    void rebalanceLocked();
    size_t numElementsLocked();

    const std::vector<unsigned int> segment_ratio;
    const unsigned int total_ratio_weight;

    std::vector<std::deque<detail::Node>> segments;
    mutable TimedMutex mutex;
};
}
