#include <memory>
#include <utility>
#include <gtest/gtest.h>

#include <Storages/DiskCache/Region.h>
#include <Storages/DiskCache/Types.h>

namespace DB::HybridCache
{
TEST(Region, ReadAndBlock)
{
    Region r{RegionId(0), 1024};

    auto desc = r.openForRead();
    EXPECT_EQ(desc.getStatus(), OpenStatus::Ready);

    EXPECT_FALSE(r.readyForReclaim(false));
    EXPECT_EQ(r.openForRead().getStatus(), OpenStatus::Retry);
    r.close(std::move(desc));
    EXPECT_TRUE(r.readyForReclaim(false));

    r.reset();
    EXPECT_EQ(r.openForRead().getStatus(), OpenStatus::Ready);
}

TEST(Region, WriteAndBlock)
{
    Region r{RegionId(0), 1024};

    auto [desc1, addr1] = r.openAndAllocate(1025);
    EXPECT_EQ(desc1.getStatus(), OpenStatus::Error);

    auto [desc2, addr2] = r.openAndAllocate(100);
    EXPECT_EQ(desc2.getStatus(), OpenStatus::Ready);
    EXPECT_FALSE(r.readyForReclaim(false));
    r.close(std::move(desc2));
    EXPECT_TRUE(r.readyForReclaim(false));

    r.reset();
    auto [desc3, addr3] = r.openAndAllocate(1024);
    EXPECT_EQ(desc3.getStatus(), OpenStatus::Ready);
}

TEST(Region, BufferAttachDetach)
{
    auto b = std::make_unique<Buffer>(1024);
    Region r{RegionId(0), 1024};
    r.attachBuffer(std::move(b));
    EXPECT_TRUE(r.hasBuffer());
    Buffer write_buf(1024);
    memset(write_buf.data(), 'A', 1024);
    Buffer read_buf(1024);
    r.writeToBuffer(0, write_buf.view());
    r.readFromBuffer(0, read_buf.mutableView());
    EXPECT_TRUE(write_buf.view() == read_buf.view());
    b = r.detachBuffer();
    EXPECT_FALSE(r.hasBuffer());
}

TEST(Region, BufferFlush)
{
    auto b = std::make_unique<Buffer>(1024);
    Region r{RegionId(0), 1024};
    r.attachBuffer(std::move(b));
    EXPECT_TRUE(r.hasBuffer());

    auto [desc2, addr2] = r.openAndAllocate(100);
    EXPECT_EQ(desc2.getStatus(), OpenStatus::Ready);

    EXPECT_EQ(Region::FlushRes::kRetryPendingWrites, r.flushBuffer([](auto, auto) { return true; }));

    r.close(std::move(desc2));
    EXPECT_EQ(Region::FlushRes::kRetryDeviceFailure, r.flushBuffer([](auto, auto) { return false; }));
    EXPECT_EQ(Region::FlushRes::kSuccess, r.flushBuffer([](auto, auto) { return true; }));
}
}
