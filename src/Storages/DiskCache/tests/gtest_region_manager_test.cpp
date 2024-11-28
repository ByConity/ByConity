#include <chrono>
#include <memory>
#include <thread>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <gtest/gtest.h>

#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/FifoPolicy.h>
#include <Storages/DiskCache/LruPolicy.h>
#include <Storages/DiskCache/Region.h>
#include <Storages/DiskCache/RegionManager.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/DiskCache/tests/BufferGen.h>
#include <Storages/DiskCache/tests/MockDevice.h>
#include <Storages/DiskCache/tests/MockJobScheduler.h>
#include <Storages/DiskCache/tests/MockPolicy.h>
#include <Storages/DiskCache/tests/SeqPoints.h>
#include "Common/CurrentMetrics.h"
#include <Common/InjectPause.h>
#include <common/types.h>

namespace DB::HybridCache
{
namespace
{
    const Region kRegion0{RegionId{0}, 100};
    const Region kRegion1{RegionId{1}, 100};
    const Region kRegion2{RegionId{2}, 100};
    const Region kRegion3{RegionId{3}, 100};
    constexpr UInt16 kFlushRetryLimit = 10;
}

TEST(RegionManager, ReclaimLruAsFifo)
{
    auto policy = std::make_unique<LruPolicy>(4);
    policy->track(kRegion0);
    policy->track(kRegion1);
    policy->track(kRegion2);
    policy->track(kRegion3);

    constexpr UInt32 k_num_regions = 4;
    constexpr UInt32 k_region_size = 4 * 1024;
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
        0,
        kFlushRetryLimit);

    EXPECT_EQ(kRegion0.id(), rm->evict());
    EXPECT_EQ(kRegion1.id(), rm->evict());
    EXPECT_EQ(kRegion2.id(), rm->evict());
    EXPECT_EQ(kRegion3.id(), rm->evict());
}

TEST(RegionManager, ReclaimLru)
{
    auto policy = std::make_unique<LruPolicy>(4);
    policy->track(kRegion0);
    policy->track(kRegion1);
    policy->track(kRegion2);
    policy->track(kRegion3);

    constexpr UInt32 k_num_regions = 4;
    constexpr UInt32 k_region_size = 4 * 1024;
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
        0,
        kFlushRetryLimit);

    rm->touch(kRegion0.id());
    rm->touch(kRegion1.id());

    EXPECT_EQ(kRegion2.id(), rm->evict());
    EXPECT_EQ(kRegion3.id(), rm->evict());
    EXPECT_EQ(kRegion0.id(), rm->evict());
    EXPECT_EQ(kRegion1.id(), rm->evict());
}


TEST(RegionManager, ReadWrite)
{
    constexpr UInt64 k_base_offset = 1024;
    constexpr UInt32 k_num_regions = 4;
    constexpr UInt32 k_region_size = 4 * 1024;

    auto device = createMemoryDevice(k_base_offset + k_num_regions * k_region_size);
    auto * device_ptr = device.get();
    RegionEvictCallback evict_callback{[](RegionId, BufferView) { return 0; }};
    RegionCleanupCallback cleanup_callback{[](RegionId, BufferView) {}};
    auto rm = std::make_unique<RegionManager>(
        k_num_regions,
        k_region_size,
        k_base_offset,
        *device,
        1,
        1,
        std::move(evict_callback),
        std::move(cleanup_callback),
        std::make_unique<FifoPolicy>(),
        k_num_regions,
        0,
        kFlushRetryLimit);

    ENABLE_INJECT_PAUSE_IN_SCOPE();
    injectPauseSet("pause_reclaim_done");

    constexpr uint32_t k_local_offset = 3 * 1024;
    constexpr uint32_t k_size = 1024;
    BufferGen bg;
    RegionId rid;
    // do reclaim couple of times to get RegionId of 1
    rm->startReclaim();
    EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
    ASSERT_EQ(OpenStatus::Ready, rm->getCleanRegion(rid, false).first);
    ASSERT_EQ(0, rid.index());
    rm->startReclaim();
    EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
    ASSERT_EQ(OpenStatus::Ready, rm->getCleanRegion(rid, false).first);
    ASSERT_EQ(1, rid.index());


    auto & region = rm->getRegion(rid);
    auto [wDesc, addr] = region.openAndAllocate(4 * k_size);
    EXPECT_EQ(OpenStatus::Ready, wDesc.getStatus());
    auto buf = bg.gen(k_size);
    auto w_addr = RelAddress{rid, k_local_offset};
    rm->write(w_addr, buf.copy());
    auto r_desc = rm->openForRead(rid, 1);
    auto buf_read = rm->read(r_desc, w_addr, k_size);
    EXPECT_TRUE(buf_read.size() == k_size);
    EXPECT_EQ(buf.view(), buf_read.view());

    // flush buffer
    region.close(std::move(wDesc));
    EXPECT_EQ(Region::FlushRes::kSuccess, rm->flushBuffer(rid));
    // Check device directly at the offset we expect data to be written
    auto expected_ofs = k_base_offset + k_region_size + k_local_offset;
    Buffer buf_read_direct{k_size};
    EXPECT_TRUE(device_ptr->read(expected_ofs, k_size, buf_read_direct.data()));
    EXPECT_EQ(buf.view(), buf_read_direct.view());
}

using testing::_;
using testing::Return;
TEST(RegionManager, cleanupRegionFailureSync)
{
    constexpr UInt32 k_num_regions = 4;
    constexpr UInt32 k_region_size = 4096;
    constexpr UInt16 k_num_in_mem_buffer = 2;
    auto device = std::make_unique<MockDevice>(k_num_regions * k_region_size, 1024);
    auto policy = std::make_unique<LruPolicy>(k_num_regions);
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
        k_num_in_mem_buffer,
        0,
        kFlushRetryLimit);

    ENABLE_INJECT_PAUSE_IN_SCOPE();
    injectPauseSet("pause_reclaim_done");
    injectPauseSet("pause_flush_failure");

    BufferGen generator;
    RegionId rid;
    rm->startReclaim();
    EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

    ASSERT_EQ(OpenStatus::Ready, rm->getCleanRegion(rid, false).first);
    ASSERT_EQ(0, rid.index());

    // Write to Region 0
    auto & region = rm->getRegion(rid);
    auto [wDesc, addr] = region.openAndAllocate(k_region_size);
    EXPECT_EQ(OpenStatus::Ready, wDesc.getStatus());
    auto buf = generator.gen(1024);
    auto w_addr = RelAddress{rid, 0};
    rm->write(w_addr, buf.copy());
    region.close(std::move(wDesc));

    SeqPoints sp;
    std::thread read_thread{[&sp, &region] {
        auto r_desc = region.openForRead();
        EXPECT_EQ(OpenStatus::Ready, r_desc.getStatus());
        sp.reached(0); // unblock flush

        sp.wait(1); // block here
        region.close(std::move(r_desc));
    }};

    std::thread flush_thread{[&sp, &device, &rm, &rid] {
        EXPECT_CALL(*device, writeImpl(_, _, _)).WillRepeatedly(Return(false));
        sp.wait(0); // Flush after active reader
        rm->doFlush(rid, false /* async */);
    }};

    std::thread count_thread{[&sp] {
        // bool retried = false;
        for (int i = 0; i < 100; i++)
        {
            // if (retried)
            // {
            //     break;
            // }
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        }
        // EXPECT_TRUE(retried);

        EXPECT_FALSE(injectPauseWait("pause_flush_failure", 1, true, 1000));

        sp.reached(1);
    }};

    // Flush can complete now
    EXPECT_TRUE(injectPauseWait("pause_flush_failure"));

    read_thread.join();
    flush_thread.join();
    count_thread.join();
}

TEST(RegionManager, cleanupRegionFailureAsync)
{
    constexpr UInt32 k_num_regions = 4;
    constexpr UInt32 k_region_size = 4096;
    constexpr UInt16 k_num_in_mem_buffer = 2;
    auto device = std::make_unique<MockDevice>(k_num_regions * k_region_size, 1024);
    auto policy = std::make_unique<LruPolicy>(k_num_regions);
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
        k_num_in_mem_buffer,
        0,
        kFlushRetryLimit);

    ENABLE_INJECT_PAUSE_IN_SCOPE();

    injectPauseSet("pause_reclaim_done");
    injectPauseSet("pause_flush_begin");
    injectPauseSet("pause_flush_failure");

    BufferGen generator;
    RegionId rid;
    rm->startReclaim();
    EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

    ASSERT_EQ(OpenStatus::Ready, rm->getCleanRegion(rid, false).first);
    ASSERT_EQ(0, rid.index());

    auto & region = rm->getRegion(rid);
    auto [wdesc, addr] = region.openAndAllocate(k_region_size);
    EXPECT_EQ(OpenStatus::Ready, wdesc.getStatus());
    auto buf = generator.gen(1024);
    auto waddr = RelAddress{rid, 0};
    rm->write(waddr, buf.copy());
    region.close(std::move(wdesc));

    SeqPoints sp;
    std::thread read_thread([&sp, &region] {
        auto rdesc = region.openForRead();
        EXPECT_EQ(OpenStatus::Ready, rdesc.getStatus());
        sp.reached(0);

        sp.wait(1);
        region.close(std::move(rdesc));
    });

    std::thread flush_thread([&sp, &device, &rm, &rid] {
        EXPECT_CALL(*device, writeImpl(_, _, _)).WillRepeatedly(Return(false));
        sp.wait(0);
        rm->doFlush(rid, true);
    });

    std::thread cthread([&sp] {
        // bool retried = false;
        EXPECT_TRUE(injectPauseWait("pause_flush_begin"));

        for (int i = 0; i < 100; i++)
        {
            // if (retried)
            // {
            //     break;
            // }
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        }

        // EXPECT_TRUE(retried);
        EXPECT_FALSE(injectPauseWait("pause_flush_failure", 1, true, 1000));
        sp.reached(1);
    });

    EXPECT_TRUE(injectPauseWait("pause_flush_failure"));

    read_thread.join();
    flush_thread.join();
    cthread.join();
}

TEST(RegionManager, Recovery)
{
    constexpr UInt32 k_num_regions = 4;
    constexpr UInt32 k_region_size = 4 * 1024;
    auto device = createMemoryDevice(k_num_regions * k_region_size);

    Buffer metadata(1024);

    {
        std::vector<UInt32> hits(4);
        auto policy = std::make_unique<MockPolicy>(&hits);
        expectRegionsTracked(*policy, {0, 1, 2, 3});
        RegionEvictCallback evict_cb{[](RegionId, BufferView) { return 0; }};
        RegionCleanupCallback cleanup_cb{[](RegionId, BufferView) {}};
        auto rm = std::make_unique<RegionManager>(
            k_num_regions,
            k_region_size,
            0,
            *device,
            1,
            1,
            std::move(evict_cb),
            std::move(cleanup_cb),
            std::move(policy),
            k_num_regions,
            0,
            kFlushRetryLimit);

        for (int i = 0; i < 20; i++)
        {
            auto [desc, addr] = rm->getRegion(RegionId{1}).openAndAllocate(101);
            rm->getRegion(RegionId{1}).close(std::move(desc));
        }
        for (int i = 0; i < 30; i++)
        {
            auto [desc, addr] = rm->getRegion(RegionId{2}).openAndAllocate(101);
            rm->getRegion(RegionId{2}).close(std::move(desc));
        }

        google::protobuf::io::ArrayOutputStream raw_stream(metadata.data(), 1024);
        google::protobuf::io::CodedOutputStream stream(&raw_stream);
        rm->persist(&stream);
    }

    {
        std::vector<UInt32> hits(4);
        auto policy = std::make_unique<MockPolicy>(&hits);
        {
            testing::InSequence s;
            EXPECT_CALL(*policy, reset());
            expectRegionsTracked(*policy, {0, 1, 2, 3});
            EXPECT_CALL(*policy, reset());
            expectRegionsTracked(*policy, {0, 3, 1, 2});
        }

        RegionEvictCallback evict_cb{[](RegionId, BufferView) { return 0; }};
        RegionCleanupCallback cleanup_cb{[](RegionId, BufferView) {}};
        auto rm = std::make_unique<RegionManager>(
            k_num_regions,
            k_region_size,
            0,
            *device,
            1,
            1,
            std::move(evict_cb),
            std::move(cleanup_cb),
            std::move(policy),
            k_num_regions,
            0,
            kFlushRetryLimit);

        google::protobuf::io::ArrayInputStream raw_stream(metadata.data(), 1024);
        google::protobuf::io::CodedInputStream stream(&raw_stream);
        rm->recover(&stream);

        EXPECT_EQ(0, rm->getRegion(RegionId{0}).getLastEntryEndOffset());
        EXPECT_EQ(0, rm->getRegion(RegionId{0}).getNumItems());

        EXPECT_EQ(2020, rm->getRegion(RegionId{1}).getLastEntryEndOffset());
        EXPECT_EQ(20, rm->getRegion(RegionId{1}).getNumItems());

        EXPECT_EQ(3030, rm->getRegion(RegionId{2}).getLastEntryEndOffset());
        EXPECT_EQ(30, rm->getRegion(RegionId{2}).getNumItems());

        EXPECT_EQ(0, rm->getRegion(RegionId{3}).getLastEntryEndOffset());
        EXPECT_EQ(0, rm->getRegion(RegionId{3}).getNumItems());
    }
}

TEST(RegionManager, RecoveryLRUOrder)
{
    constexpr UInt32 k_num_regions = 4;
    constexpr UInt32 k_region_size = 4 * 1024;
    auto device = createMemoryDevice(k_num_regions * k_region_size);

    Buffer metadata(1024);

    {
        auto policy = std::make_unique<LruPolicy>(k_num_regions);
        RegionEvictCallback evict_cb{[](RegionId, BufferView) { return 0; }};
        RegionCleanupCallback cleanup_cb{[](RegionId, BufferView) {}};
        auto rm = std::make_unique<RegionManager>(
            k_num_regions,
            k_region_size,
            0,
            *device,
            1,
            1,
            std::move(evict_cb),
            std::move(cleanup_cb),
            std::move(policy),
            k_num_regions,
            0,
            kFlushRetryLimit);

        for (int i = 0; i < 10; i++)
        {
            auto [desc, addr] = rm->getRegion(RegionId{0}).openAndAllocate(200);
            rm->getRegion(RegionId{0}).close(std::move(desc));
        }
        for (int i = 0; i < 20; i++)
        {
            auto [desc, addr] = rm->getRegion(RegionId{3}).openAndAllocate(150);
            rm->getRegion(RegionId{3}).close(std::move(desc));
        }

        google::protobuf::io::ArrayOutputStream raw_stream(metadata.data(), 1024);
        google::protobuf::io::CodedOutputStream stream(&raw_stream);
        rm->persist(&stream);
    }

    {
        auto policy = std::make_unique<LruPolicy>(k_num_regions);
        RegionEvictCallback evict_cb{[](RegionId, BufferView) { return 0; }};
        RegionCleanupCallback cleanup_cb{[](RegionId, BufferView) {}};
        auto rm = std::make_unique<RegionManager>(
            k_num_regions,
            k_region_size,
            0,
            *device,
            1,
            1,
            std::move(evict_cb),
            std::move(cleanup_cb),
            std::move(policy),
            k_num_regions,
            0,
            kFlushRetryLimit);

        google::protobuf::io::ArrayInputStream raw_stream(metadata.data(), 1024);
        google::protobuf::io::CodedInputStream stream(&raw_stream);
        rm->recover(&stream);

        EXPECT_EQ(RegionId{1}, rm->evict());
        EXPECT_EQ(RegionId{2}, rm->evict());
        EXPECT_EQ(RegionId{0}, rm->evict());
        EXPECT_EQ(RegionId{3}, rm->evict());
        EXPECT_EQ(RegionId{}, rm->evict());
    }
}
}
