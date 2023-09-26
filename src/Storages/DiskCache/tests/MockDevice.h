#pragma once

#include <Storages/DiskCache/Device.h>
#include <gmock/gmock.h>

namespace DB::HybridCache
{

class MockDevice : public Device
{
public:
    MockDevice(UInt64 device_size, UInt32 io_align_size)
        : Device{device_size, io_align_size, 0}, device_{device_size == 0 ? nullptr : createMemoryDevice(device_size, io_align_size)}
    {
        ON_CALL(*this, readImpl(testing::_, testing::_, testing::_))
            .WillByDefault(testing::Invoke([this](uint64_t offset, uint32_t size, void * buffer) {
                chassert(size % getIOAlignmentSize() == 0u);
                chassert(offset % getIOAlignmentSize() == 0u);
                return device_->read(offset, size, buffer);
            }));

        ON_CALL(*this, writeImpl(testing::_, testing::_, testing::_))
            .WillByDefault(testing::Invoke([this](uint64_t offset, uint32_t size, const void * data) {
                chassert(size % getIOAlignmentSize() == 0u);
                chassert(offset % getIOAlignmentSize() == 0u);
                Buffer buffer = device_->makeIOBuffer(size);
                std::memcpy(buffer.data(), data, size);
                return device_->write(offset, std::move(buffer));
            }));

        ON_CALL(*this, flushImpl()).WillByDefault(testing::Invoke([this]() { device_->flush(); }));
    }

    MOCK_METHOD3(readImpl, bool(UInt64, UInt32, void *));
    MOCK_METHOD3(writeImpl, bool(UInt64, UInt32, const void *));
    MOCK_METHOD0(flushImpl, void());

    Device & getRealDeviceRef() { return *device_; }

    void setRealDevice(std::unique_ptr<Device> device) { device_ = std::move(device); }

    std::unique_ptr<Device> releaseRealDevice() { return std::move(device_); }

private:
    std::unique_ptr<Device> device_;
};

class SizeMockDevice : public Device
{
public:
    explicit SizeMockDevice(uint64_t device_size) : Device(device_size) { }

    bool writeImpl(uint64_t, uint32_t, const void *) override { return false; }
    bool readImpl(uint64_t, uint32_t, void *) override { return false; }
    void flushImpl() override { }
};

}
