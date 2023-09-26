#pragma once

#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/File.h>
#include <Storages/DiskCache/Types.h>
#include <fmt/core.h>
#include <sys/param.h>
#include <Poco/AtomicCounter.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <common/types.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace DB::HybridCache
{
// Abstract device for serving read/write operations
class Device
{
public:
    explicit Device(UInt64 size) : Device{size, 0} { }

    Device(UInt64 size, UInt32 max_write_size) : Device(size, kDefaultAlignmentSize, max_write_size) { }

    Device(UInt64 size, UInt32 io_align_size, UInt32 max_write_size);

    virtual ~Device() = default;

    size_t getIOAlignedSize(size_t size) const { return roundup(size, io_alignment_size_); }

    Buffer makeIOBuffer(size_t size) const { return Buffer{getIOAlignedSize(size), io_alignment_size_}; }

    bool write(UInt64 offset, Buffer value);

    bool write(UInt64 offset, BufferView value);

    bool read(UInt64 offset, UInt32 size, void * value);

    Buffer read(UInt64 offset, UInt32 size);

    void flush() { flushImpl(); }

    UInt64 getSize() const { return size_; }

    UInt32 getIOAlignmentSize() const { return io_alignment_size_; }

    static Poco::Logger & logger();

protected:
    virtual bool writeImpl(UInt64 offset, UInt32 size, const void * value) = 0;
    virtual bool readImpl(UInt64 offset, UInt32 size, void * value) = 0;
    virtual void flushImpl() = 0;

private:
    static Poco::Logger * logger_;

    bool readInternal(UInt64 offset, UInt32 size, void * data);

    bool writeInternal(UInt64 offset, const UInt8 * data, size_t size);

    const UInt64 size_{0};

    const UInt32 io_alignment_size_{kDefaultAlignmentSize};

    // For chunking write, split large io into smaller writes.
    // 0 meanings no write limit.
    const UInt32 max_write_size_{0};

    static constexpr UInt32 kDefaultAlignmentSize{1};
};

std::unique_ptr<Device> createMemoryDevice(UInt64 size, UInt32 io_align_size = 1);

std::unique_ptr<Device> createDirectIoFileDevice(
    std::vector<File> f_vec,
    UInt64 file_size,
    UInt32 block_size,
    UInt32 stripe_size,
    UInt32 max_device_write_size,
    IoEngine io_engine,
    UInt32 q_depth);

std::unique_ptr<Device>
createDirectIoFileDevice(std::vector<File> f_vec, UInt64 file_size, UInt32 block_size, UInt32 stripe_size, UInt32 max_device_write_size);

inline Poco::Logger & Device::logger()
{
    chassert(logger_ != nullptr);
    return *logger_;
}
}
