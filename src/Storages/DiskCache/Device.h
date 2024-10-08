#pragma once

#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/Types.h>
#include <fmt/core.h>
#include <sys/param.h>
#include <Poco/AtomicCounter.h>
#include <Common/Exception.h>
#include <common/types.h>
#include <Common/File.h>

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
    explicit Device(UInt64 size_) : Device{size_, 0} { }

    Device(UInt64 size_, UInt32 max_write_size_) : Device(size_, kDefaultAlignmentSize, max_write_size_) { }

    Device(UInt64 size_, UInt32 io_align_size_, UInt32 max_write_size_);

    virtual ~Device() = default;

    size_t getIOAlignedSize(size_t size_) const { return roundup(size_, io_alignment_size); }

    Buffer makeIOBuffer(size_t size_) const { return Buffer{getIOAlignedSize(size_), io_alignment_size}; }

    bool write(UInt64 offset, Buffer value);

    bool write(UInt64 offset, BufferView value);

    bool read(UInt64 offset, UInt32 size, void * value);

    Buffer read(UInt64 offset, UInt32 size);

    void flush() { flushImpl(); }

    UInt64 getSize() const { return size; }

    UInt32 getIOAlignmentSize() const { return io_alignment_size; }

protected:
    virtual bool writeImpl(UInt64 offset, UInt32 size, const void * value) = 0;
    virtual bool readImpl(UInt64 offset, UInt32 size, void * value) = 0;
    virtual void flushImpl() = 0;

private:
    bool readInternal(UInt64 offset, UInt32 size, void * data);

    bool writeInternal(UInt64 offset, const UInt8 * data, size_t size);

    const UInt64 size{0};

    const UInt32 io_alignment_size{kDefaultAlignmentSize};

    // For chunking write, split large io into smaller writes.
    // 0 meanings no write limit.
    const UInt32 max_write_size{0};

    static constexpr UInt32 kDefaultAlignmentSize{1};
};

std::unique_ptr<Device> createMemoryDevice(UInt64 size, UInt32 io_align_size = 1);

std::unique_ptr<Device> createDirectIoFileDevice(
    std::vector<DB::File> f_vec,
    UInt64 file_size,
    UInt32 block_size,
    UInt32 stripe_size,
    UInt32 max_device_write_size,
    IoEngine io_engine = IoEngine::Sync,
    UInt32 q_depth = 0);

}
