#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Storages/DiskCache/Allocator.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/Region.h>
#include <Storages/DiskCache/RegionManager.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/DiskCache/tests/MockJobScheduler.h>
#include <Storages/DiskCache/tests/MockPolicy.h>
#include <Common/InjectPause.h>
#include <common/types.h>

namespace DB::HybridCache
{
namespace
{
    constexpr UInt16 kNoPriority = 0;
    constexpr UInt16 kNumPriorities = 1;
    constexpr UInt16 kFlushRetryLimit = 10;
}

TEST(Allocator, RegionSyncInMemBuffers)
{
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<MockPolicy>(&hits);
    constexpr UInt32 k_num_regions = 4;
    constexpr UInt32 k_region_size = 16 * 1024;
    auto device = createMemoryDevice(k_num_regions * k_region_size);
    RegionEvictCallback evict_callback{[](RegionId, BufferView) { return 0; }};
    RegionCleanupCallback cleanup_callback{[](RegionId, BufferView) {}};
    auto rm = std::make_unique<RegionManager>(
        k_num_regions,
        k_region_size,
        0,
        *device,
        1,
        1,
        std::move(evict_callback),
        std::move(cleanup_callback),
        std::move(policy),
        3,
        0,
        kFlushRetryLimit);
    Allocator allocator{*rm, kNumPriorities};

    ENABLE_INJECT_PAUSE_IN_SCOPE();
    injectPauseSet("pause_reclaim_done");
    injectPauseSet("pause_flush_begin");

    RelAddress addr;
    UInt32 slot_size = 0;

    {
        RegionDescriptor desc{OpenStatus::Retry};
        std::tie(desc, slot_size, addr) = allocator.allocate(1024, kNoPriority, false);
        EXPECT_EQ(OpenStatus::Retry, desc.getStatus());
        EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
    }


    for (UInt32 i = 0; i < 3; i++)
    {
        {
            RegionDescriptor desc{OpenStatus::Retry};
            std::tie(desc, slot_size, addr) = allocator.allocate(1024, kNoPriority, false);
            EXPECT_TRUE(desc.isReady());

            if (i > 0)
                EXPECT_TRUE(injectPauseWait("pause_flush_begin"));

            EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

            EXPECT_EQ(RegionId{i}, addr.rid());
            EXPECT_EQ(0, addr.offset());
            rm->close(std::move(desc));
        }

        for (UInt32 j = 0; j < 15; j++)
        {
            RegionDescriptor desc{OpenStatus::Retry};
            std::tie(desc, slot_size, addr) = allocator.allocate(1024, kNoPriority, false);
            EXPECT_TRUE(desc.isReady());
            EXPECT_EQ(RegionId{i}, addr.rid());
            EXPECT_EQ(1024 * (j + 1), addr.offset());
            rm->close(std::move(desc));
        }
    }

    for (UInt32 i = 0; i < 2; i++)
    {
        EXPECT_EQ(16, rm->getRegion(RegionId{i}).getNumItems());
        EXPECT_EQ(1024 * 16, rm->getRegion(RegionId{i}).getLastEntryEndOffset());
    }
    EXPECT_EQ(0, rm->getRegion(RegionId{3}).getNumItems());
    EXPECT_EQ(0, rm->getRegion(RegionId{3}).getLastEntryEndOffset());

    {
        RegionDescriptor desc{OpenStatus::Retry};
        do
            std::tie(desc, slot_size, addr) = allocator.allocate(1024, kNoPriority, false);
        while (OpenStatus::Retry == desc.getStatus());
        EXPECT_TRUE(desc.isReady());
        EXPECT_EQ(RegionId{3}, addr.rid());
        EXPECT_EQ(0, addr.offset());
        EXPECT_TRUE(injectPauseWait("pause_flush_begin"));
        EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
        rm->close(std::move(desc));
    }

    EXPECT_EQ(1, rm->getRegion(RegionId{3}).getNumItems());
    EXPECT_EQ(1024, rm->getRegion(RegionId{3}).getLastEntryEndOffset());
}

TEST(Allocator, TestInMemBufferStates)
{
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<MockPolicy>(&hits);
    constexpr UInt32 k_num_regions = 4;
    constexpr UInt32 k_region_size = 16 * 1024;
    auto device = createMemoryDevice(k_num_regions * k_region_size);

    std::vector<UInt32> size_classes{1024};
    RegionEvictCallback evict_callback{[](RegionId, BufferView) { return 0; }};
    RegionCleanupCallback cleanup_callback{[](RegionId, BufferView) {}};
    auto rm = std::make_unique<RegionManager>(
        k_num_regions,
        k_region_size,
        0,
        *device,
        1,
        1,
        std::move(evict_callback),
        std::move(cleanup_callback),
        std::move(policy),
        3,
        0,
        kFlushRetryLimit);
    Allocator allocator{*rm, kNumPriorities};

    ENABLE_INJECT_PAUSE_IN_SCOPE();

    injectPauseSet("pause_reclaim_done");
    injectPauseSet("pause_flush_begin");
    injectPauseSet("pause_flush_detach_buffer");
    injectPauseSet("pause_flush_done");

    RelAddress addr;
    UInt32 slot_size = 0;
    {
        RegionDescriptor desc{OpenStatus::Retry};
        std::tie(desc, slot_size, addr) = allocator.allocate(1024, kNoPriority, false);
        EXPECT_EQ(OpenStatus::Retry, desc.getStatus());
    }

    EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

    {
        RegionDescriptor rdesc{OpenStatus::Error};
        {
            RegionDescriptor wdesc{OpenStatus::Retry};
            std::tie(wdesc, slot_size, addr) = allocator.allocate(1024, kNoPriority, false);
            EXPECT_TRUE(wdesc.isReady());
            EXPECT_EQ(0, wdesc.id().index());
            EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

            rdesc = rm->openForRead(RegionId{0}, 2);
            EXPECT_TRUE(rdesc.isReady());
            for (UInt32 j = 0; j < 15; j++)
            {
                RegionDescriptor desc{OpenStatus::Retry};
                std::tie(desc, slot_size, addr) = allocator.allocate(1024, kNoPriority, false);
                EXPECT_TRUE(desc.isReady());
                EXPECT_EQ(0, desc.id().index());
                rm->close(std::move(desc));
            }
            EXPECT_FALSE(injectPauseWait("pause_reclaim_done", 1, true, 1000));

            {
                RegionDescriptor desc{OpenStatus::Retry};
                std::tie(desc, slot_size, addr) = allocator.allocate(1024, kNoPriority, false);
                EXPECT_EQ(OpenStatus::Ready, desc.getStatus());
                EXPECT_EQ(1, desc.id().index());
                rm->close(std::move(desc));
            }
            EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
            EXPECT_TRUE(injectPauseWait("pause_flush_begin"));
            EXPECT_FALSE(injectPauseWait("pause_flush_detach_buffer", 1, true, 1000));

            EXPECT_FALSE(rm->getRegion(RegionId{0}).isFlushedLocked());
            rm->close(std::move(wdesc));
        }

        EXPECT_TRUE(injectPauseWait("pause_flush_detach_buffer"));
        EXPECT_FALSE(injectPauseWait("pause_flush_done", 1 /* numThreads */, true /* wakeup */, 1000 /* timeoutMs */));
        EXPECT_TRUE(rm->getRegion(RegionId{0}).isFlushedLocked());
        rm->close(std::move(rdesc));
    }
    EXPECT_TRUE(injectPauseWait("pause_flush_done"));
}

TEST(Allocator, UsePriorities)
{
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<MockPolicy>(&hits);
    constexpr UInt32 k_num_regions = 4;
    constexpr UInt32 k_region_size = 16 * 1024;
    auto device = createMemoryDevice(k_num_regions * k_region_size);
    RegionEvictCallback evict_callback{[](RegionId, BufferView) { return 0; }};
    RegionCleanupCallback cleanup_callback{[](RegionId, BufferView) {}};
    auto rm = std::make_unique<RegionManager>(
        k_num_regions,
        k_region_size,
        0,
        *device,
        1,
        1,
        std::move(evict_callback),
        std::move(cleanup_callback),
        std::move(policy),
        k_num_regions,
        3,
        kFlushRetryLimit);

    Allocator allocator{*rm, 3};

    ENABLE_INJECT_PAUSE_IN_SCOPE();

    injectPauseSet("pause_reclaim_done");

    auto [desc, slot_size, addr] = allocator.allocate(1024, 0, false);
    EXPECT_EQ(OpenStatus::Retry, desc.getStatus());
    EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

    for (UInt16 pri = 0; pri < 3; pri++)
    {
        std::tie(desc, slot_size, addr) = allocator.allocate(1024, pri, false);
        EXPECT_TRUE(desc.isReady());
        EXPECT_EQ(RegionId{pri}, addr.rid());
        EXPECT_EQ(pri, rm->getRegion(addr.rid()).getPriority());
        EXPECT_EQ(0, addr.offset());
        EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
    }
}
}
