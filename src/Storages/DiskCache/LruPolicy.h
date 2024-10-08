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

class LruPolicy final : public EvictionPolicy
{
public:
    explicit LruPolicy(UInt32 expected_num_regions);

    LruPolicy(const LruPolicy &) = delete;
    LruPolicy & operator=(const LruPolicy &) = delete;

    ~LruPolicy() override = default;

    void touch(RegionId rid) override;

    void track(const Region & region) override;

    RegionId evict() override;

    void reset() override;

    size_t memorySize() const override;

private:
    LoggerPtr log = getLogger("LruPolicy");

    static constexpr UInt32 kInvalidIndex = 0xffffffffu;

    struct ListNode
    {
        UInt32 prev{kInvalidIndex};
        UInt32 next{kInvalidIndex};

        std::chrono::seconds creation_time{};
        std::chrono::seconds last_update_time{};

        UInt32 hits{};

        bool inList() const { return prev != kInvalidIndex || next != kInvalidIndex; }

        std::chrono::seconds secondsSinceCreation() const { return getSteadyClockSeconds() - creation_time; }

        std::chrono::seconds secondsSinceAccess() const { return getSteadyClockSeconds() - last_update_time; }
    };

    void unlink(UInt32 i);
    void linkAtHead(UInt32 i);
    void linkAtTail(UInt32 i);

    std::vector<ListNode> array;
    UInt32 head{kInvalidIndex};
    UInt32 tail{kInvalidIndex};
    mutable TimedMutex mutex;
};
}
