#pragma once

#include <chrono>
#include <mutex>

#include <Poco/Logger.h>

#include <Storages/DiskCache/EvictionPolicy.h>
#include <Storages/DiskCache/Types.h>
#include <common/chrono_io.h>

namespace DB::HybridCache
{
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
        std::lock_guard<std::mutex> guard{mutex};
        return sizeof(*this) + sizeof(detail::Node) * queue.size();
    }

private:
    Poco::Logger * log = &Poco::Logger::get("FifoPolicy");

    std::deque<detail::Node> queue;
    mutable std::mutex mutex;
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
    mutable std::mutex mutex;
};
}
