#include <numeric>

#include <fmt/core.h>

#include <Storages/DiskCache/FifoPolicy.h>
#include <Storages/DiskCache/Types.h>
#include <Common/Exception.h>
#include <common/chrono_io.h>
#include <common/logger_useful.h>

namespace DB::HybridCache
{
namespace detail
{
    unsigned int accumulate(const std::vector<unsigned int> nums)
    {
        return std::accumulate(nums.begin(), nums.end(), 0u, [](unsigned int a, unsigned int b) {
            if (b == 0)
                throw std::invalid_argument(fmt::format("Expected non-zero element. Actual: {}", b));

            return a + b;
        });
    }
}

FifoPolicy::FifoPolicy()
{
    LOG_INFO(log, "FIFO policy");
}

void FifoPolicy::track(const Region & region)
{
    std::lock_guard<TimedMutex> guard{mutex};
    queue.push_back(detail::Node{region.id(), getSteadyClockSeconds()});
}

RegionId FifoPolicy::evict()
{
    std::lock_guard<TimedMutex> guard{mutex};
    if (queue.empty())
        return RegionId{};
    auto rid = queue.front().rid;
    queue.pop_front();
    return rid;
}

void FifoPolicy::reset()
{
    std::lock_guard<TimedMutex> guard{mutex};
    queue.clear();
}

SegmentedFifoPolicy::SegmentedFifoPolicy(std::vector<unsigned int> segment_ratio_)
    : segment_ratio{std::move(segment_ratio_)}, total_ratio_weight{detail::accumulate(segment_ratio)}, segments{segment_ratio.size()}
{
    if (segments.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "invalid segments");
    chassert(total_ratio_weight > 0u);
}

void SegmentedFifoPolicy::track(const Region & region)
{
    auto priority = region.getPriority();
    chassert(priority < segments.size());
    std::lock_guard<TimedMutex> lock{mutex};
    segments[priority].push_back(detail::Node{region.id(), getSteadyClockSeconds()});
    rebalanceLocked();
}

RegionId SegmentedFifoPolicy::evict()
{
    std::lock_guard<TimedMutex> lock{mutex};
    auto & lowest_pri = segments.front();
    if (lowest_pri.empty())
    {
        chassert(numElementsLocked() == 0ul);
        return RegionId{};
    }
    auto rid = lowest_pri.front().rid;
    lowest_pri.pop_front();
    rebalanceLocked();
    return rid;
}

void SegmentedFifoPolicy::rebalanceLocked()
{
    auto regions_tracked = numElementsLocked();

    auto curr_segment = segments.rbegin();
    auto curr_segment_ratio = segment_ratio.rbegin();
    auto next_segment = std::next(curr_segment);
    while (next_segment != segments.rend())
    {
        auto curr_segment_limit = regions_tracked * *curr_segment_ratio / total_ratio_weight;
        while (curr_segment_limit < curr_segment->size())
        {
            next_segment->push_back(curr_segment->front());
            curr_segment->pop_front();
        }

        curr_segment = next_segment;
        curr_segment_ratio++;
        next_segment++;
    }
}

size_t SegmentedFifoPolicy::numElementsLocked()
{
    return std::accumulate(segments.begin(), segments.end(), 0ul, [](size_t size, const auto & segment) { return size + segment.size(); });
}

void SegmentedFifoPolicy::reset()
{
    std::lock_guard<TimedMutex> lock{mutex};
    for (auto & segment : segments)
        segment.clear();
}

size_t SegmentedFifoPolicy::memorySize() const
{
    size_t mem_size = sizeof(*this);
    std::lock_guard<TimedMutex> lock{mutex};
    for (const auto & segment : segments)
        mem_size += sizeof(std::deque<detail::Node>) + sizeof(detail::Node) * segment.size();
    return mem_size;
}
}
