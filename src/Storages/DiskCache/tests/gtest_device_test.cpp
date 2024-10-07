#include <filesystem>
#include <random>
#include <fcntl.h>
#include <unistd.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/tests/MockDevice.h>
#include <Common/thread_local_rng.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>

namespace ProfileEvents
{
extern const Event DiskCacheDeviceBytesWritten;
}

namespace DB::HybridCache
{
using testing::_;

TEST(Device, BytesWritten)
{
    MockDevice device{100, 1};
    EXPECT_CALL(device, writeImpl(_, _, _))
        .WillOnce(testing::Return(true))
        .WillOnce(testing::Return(true))
        .WillOnce(testing::Return(false));

    EXPECT_TRUE(device.write(0, Buffer{5}));
    EXPECT_TRUE(device.write(0, Buffer{9}));
    EXPECT_FALSE(device.write(0, Buffer{1}));
}

TEST(Device, IOError)
{
    MockDevice device{1, 1};
    EXPECT_CALL(device, readImpl(0, 1, _)).WillOnce(testing::InvokeWithoutArgs([] { return false; }));
    EXPECT_CALL(device, writeImpl(0, 1, _)).WillOnce(testing::InvokeWithoutArgs([] { return false; }));

    Buffer buf{1};
    device.read(0, 1, nullptr);
    device.write(0, std::move(buf));
}

struct DeviceParamTest : public testing::TestWithParam<std::tuple<IoEngine, int>>
{
    DeviceParamTest() : io_engine_(std::get<0>(GetParam())), q_depth_(std::get<1>(GetParam()))
    {
        LOG_INFO(getLogger("DeviceParamTest"), "ioEngine={}, qDepth={}", getIoEngineName(io_engine_), q_depth_);
    }

protected:
    std::shared_ptr<Device>
    createFileDevice(std::vector<File> f_vec, UInt64 file_size, UInt32 block_size, UInt32 stripe_size, UInt32 max_device_write_size)
    {
        device_
            = createDirectIoFileDevice(std::move(f_vec), file_size, block_size, stripe_size, max_device_write_size, io_engine_, q_depth_);
        return device_;
    }

    std::shared_ptr<Device> getDevice() const { return device_; }

    IoEngine io_engine_;
    UInt32 q_depth_;
    std::shared_ptr<Device> device_;
};

TEST_P(DeviceParamTest, MaxWriteTest)
{
    auto file_path = fmt::format("/tmp/DEVICE_MAXWRITE_TEST-{}", ::getpid());

    int device_size = 16 * 1024;
    int io_align_size = 1024;

    std::vector<File> f_vec;
    f_vec.emplace_back(File(file_path, O_RDWR | O_CREAT, S_IRWXU));

    auto device = createFileDevice(std::move(f_vec), device_size, io_align_size, io_align_size, 1024);

    UInt32 buf_size = 4 * 1024;
    Buffer wbuf = device->makeIOBuffer(buf_size);
    Buffer rbuf = device->makeIOBuffer(buf_size);
    auto * wdata = wbuf.data();
    std::uniform_int_distribution<UInt32> dist(INT_MIN, INT_MAX);
    for (UInt32 i = 0; i < buf_size; i++)
    {
        wdata[i] = dist(thread_local_rng) % 64;
    }
    auto ret = device->write(0, wbuf.copy(io_align_size));
    EXPECT_EQ(true, ret);

    ret = device->read(0, buf_size, rbuf.data());
    EXPECT_EQ(true, ret);
    auto * rdata = rbuf.data();
    for (UInt32 i = 0; i < buf_size; i++)
    {
        EXPECT_EQ(wdata[i], rdata[i]);
    }
}

namespace fs = std::filesystem;

TEST_P(DeviceParamTest, RAID0IO)
{
    auto file_path = fmt::format("/tmp/DEVICE_RAID0IO_TEST-{}", ::getpid());

    fs::create_directory(file_path);
    SCOPE_EXIT({ fs::remove_all(fs::path{file_path}); });

    std::vector<std::string> files = {file_path + "/CACHE0", file_path + "/CACHE1", file_path + "/CACHE2", file_path + "/CACHE3"};

    int size = 4 * 1024 * 1024;
    int io_align_size = 4096;
    int stripe_size = 8192;

    std::vector<File> fvec;
    for (const auto & file : files)
    {
        auto f = File(file.c_str(), O_RDWR | O_CREAT);
        auto ret = ::fallocate(f.getFd(), 0, 0, size);
        EXPECT_EQ(0, ret);
        fvec.push_back(std::move(f));
    }
    auto vec_size = fvec.size();
    auto device = createFileDevice(std::move(fvec), size, io_align_size, stripe_size, 0);

    EXPECT_EQ(vec_size * size, device->getSize());

    // Simple IO
    {
        Buffer wbuf = device->makeIOBuffer(stripe_size);
        Buffer rbuf = device->makeIOBuffer(stripe_size);
        std::memset(wbuf.data(), 'A', stripe_size);
        auto ret = device->write(0, wbuf.copy(io_align_size));
        EXPECT_EQ(true, ret);
        ret = device->read(0, stripe_size, rbuf.data());
        EXPECT_EQ(true, ret);
        auto rc = std::memcmp(wbuf.data(), rbuf.data(), stripe_size);
        EXPECT_EQ(0, rc);
    }
    // IO spans two stripes
    {
        Buffer wbuf = device->makeIOBuffer(stripe_size);
        Buffer rbuf = device->makeIOBuffer(stripe_size);
        std::memset(wbuf.data(), 'B', stripe_size);
        auto ret = device->write(io_align_size, wbuf.copy(io_align_size));
        EXPECT_EQ(true, ret);
        ret = device->read(io_align_size, stripe_size, rbuf.data());
        EXPECT_EQ(true, ret);
        auto rc = std::memcmp(wbuf.data(), rbuf.data(), stripe_size);
        EXPECT_EQ(0, rc);
    }
    // IO spans several stripes
    {
        auto io_size = 10 * stripe_size;
        auto offset = stripe_size * 7 + io_align_size;
        Buffer wbuf = device->makeIOBuffer(io_size);
        Buffer rbuf = device->makeIOBuffer(io_size);
        std::memset(wbuf.data(), 'C', stripe_size);
        auto ret = device->write(offset, wbuf.copy(io_align_size));
        EXPECT_EQ(true, ret);
        ret = device->read(offset, io_size, rbuf.data());
        EXPECT_EQ(true, ret);
        auto rc = std::memcmp(wbuf.data(), rbuf.data(), io_size);
        EXPECT_EQ(0, rc);
    }
    // IO size < stripeSize
    {
        auto io_size = stripe_size / 2;
        auto offset = stripe_size * 22 + io_align_size;
        Buffer wbuf = device->makeIOBuffer(io_size);
        Buffer rbuf = device->makeIOBuffer(io_size);
        std::memset(wbuf.data(), 'D', io_size);
        auto ret = device->write(offset, wbuf.copy(io_align_size));
        EXPECT_EQ(true, ret);
        ret = device->read(offset, io_size, rbuf.data());
        EXPECT_EQ(true, ret);
        auto rc = std::memcmp(wbuf.data(), rbuf.data(), io_size);
        EXPECT_EQ(0, rc);
    }
}

TEST_P(DeviceParamTest, RAID0IOAlignment)
{
    auto file_path = fmt::format("/tmp/DEVICE_RAID0IO_TEST-{}", ::getpid());
    fs::create_directories(file_path);
    SCOPE_EXIT({ fs::remove_all(fs::path{file_path}); });

    std::vector<std::string> files = {file_path + "/CACHE0", file_path + "/CACHE1", file_path + "/CACHE2", file_path + "/CACHE3"};

    int size = 4 * 1024 * 1024;
    int io_align_size = 4096;
    int stripe_size = 8192;

    std::vector<File> fvec;
    for (const auto & file : files)
    {
        auto f = File(file.c_str(), O_RDWR | O_CREAT);
        auto ret = ::fallocate(f.getFd(), 0, 0, size);
        EXPECT_EQ(0, ret);
        fvec.push_back(std::move(f));
    }

    size = 2 * 1024 * 1024 + stripe_size / fvec.size();
    ASSERT_THROW(createFileDevice(std::move(fvec), size, io_align_size, stripe_size, 0), DB::Exception);
}

INSTANTIATE_TEST_SUITE_P(DeviceParamTestSuite_Sync, DeviceParamTest, testing::Values(std::make_tuple(IoEngine::Sync, 0)));
// INSTANTIATE_TEST_SUITE_P(DeviceParamTestSuite_Uring_1, DeviceParamTest, testing::Values(std::make_tuple(IoEngine::IoUring, 1)));
// INSTANTIATE_TEST_SUITE_P(DeviceParamTestSuite_Uring_32, DeviceParamTest, testing::Values(std::make_tuple(IoEngine::IoUring, 32)));
// INSTANTIATE_TEST_SUITE_P(DeviceParamTestSuite_Hybrid, DeviceParamTest, testing::Values(std::make_tuple(IoEngine::Sync, 0),
//                                                                                         std::make_tuple(IoEngine::IoUring, 1),
//                                                                                         std::make_tuple(IoEngine::IoUring, 32)));

}
