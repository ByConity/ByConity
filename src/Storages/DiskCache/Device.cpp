#include <cstddef>
#include <cstring>
#include <filesystem>
#include <memory>
#include <vector>
#include <unistd.h>
#include <sys/types.h>

#include <fmt/core.h>
#include <Poco/Logger.h>

#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/Types.h>
#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <common/chrono_io.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace ProfileEvents
{
extern const Event DiskCacheDeviceBytesWritten;
extern const Event DiskCacheDeviceBytesRead;
extern const Event DiskCacheDeviceWriteIOErrors;
extern const Event DiskCacheDeviceReadIOErrors;
extern const Event DiskCacheDeviceWriteIOLatency;
extern const Event DiskCacheDeviceReadIOLatency;
}

namespace DB::HybridCache
{
Poco::Logger * Device::logger_{nullptr};

Device::Device(UInt64 size_, UInt32 io_align_size_, UInt32 max_write_size_)
    : size(size_), io_alignment_size(io_align_size_), max_write_size(max_write_size_)
{
    if (io_align_size_ == 0)
        throwFromErrno(fmt::format("Invalid io_align_size: {}", io_align_size_), ErrorCodes::BAD_ARGUMENTS);

    if (max_write_size % io_alignment_size != 0)
        throwFromErrno(
            fmt::format("Invalid max_write_size: {}, io_align_size: {}", max_write_size, io_alignment_size), ErrorCodes::BAD_ARGUMENTS);

    logger_ = &Poco::Logger::get("Device");
}

namespace
{
    struct IOReq;
    class IoContext;
    class AsyncIoContext;
    class FileDevice;

    constexpr size_t kIOTimeoutMs = 1000;

    enum OpType : UInt8
    {
        INVALID = 0,
        READ,
        WRITE
    };

    struct IOOp
    {
        explicit IOOp(IOReq & parent_, int idx_, int fd_, UInt64 offset_, UInt32 size_, void * data_)
            : parent(parent_), idx(idx_), fd(fd_), offset(offset_), size(size_), data(data_)
        {
        }

        std::string toString() const;

        bool done(ssize_t status);

        IOReq & parent;

        const UInt32 idx;

        const int fd;
        const UInt64 offset = 0;
        const UInt32 size = 0;
        void * const data;

        UInt8 resubmitted = 0;

        std::chrono::nanoseconds start_time;
        std::chrono::nanoseconds submit_time;
    };

    struct IOReq
    {
        explicit IOReq(
            IoContext & context,
            const std::vector<File> & fvec,
            UInt32 stripe_size,
            OpType op_type,
            UInt64 offset,
            UInt32 size,
            void * data);

        const char * getOpName() const
        {
            switch (op_type)
            {
                case OpType::READ:
                    return "read";
                case OpType::WRITE:
                    return "write";
                default:
                    chassert(false);
            }
            return "unknown";
        }

        bool waitCompletion();

        void notifyOpResult(bool result);

        std::string toString() const
        {
            return fmt::format(
                "[req {}] {} offset {} size {} data {} ops {} remaining {} result {}]",
                reinterpret_cast<const void *>(this),
                getOpName(),
                offset,
                size,
                data,
                ops.size(),
                num_remaining,
                result);
        }

        IoContext & context;
        const OpType op_type = OpType::INVALID;
        const UInt64 offset = 0;
        const UInt32 size = 0;
        void * const data;

        bool result = true;

        UInt32 num_remaining = 0;
        std::vector<IOOp> ops;

        std::chrono::nanoseconds start_time;
        std::chrono::nanoseconds comp_time;
    };

    class IoContext
    {
    public:
        IoContext() = default;
        virtual ~IoContext() = default;

        virtual std::string getName() = 0;

        virtual bool isAsyncIoCompletion() = 0;

        std::shared_ptr<IOReq> submitRead(const std::vector<File> & fvec, UInt32 stripe_size, UInt64 offset, UInt32 size, void * data);

        std::shared_ptr<IOReq>
        submitWrite(const std::vector<File> & fvec, UInt32 stripe_size, UInt64 offset, UInt32 size, const void * data);

        virtual bool submitIo(IOOp & op) = 0;

    protected:
        void submitReq(std::shared_ptr<IOReq> req);
    };

    class SyncIoContext : public IoContext
    {
    public:
        SyncIoContext() = default;

        std::string getName() override { return "sync"; }
        bool isAsyncIoCompletion() override { return false; }

        bool submitIo(IOOp & op) override;

    private:
        static ssize_t writeSync(int fd, UInt64 offset, UInt32 size, const void * value);
        static ssize_t readSync(int fd, UInt64 offset, UInt32 size, void * value);
    };


    class FileDevice : public Device
    {
    public:
        FileDevice(
            std::vector<DB::File> && fvec,
            UInt64 size,
            UInt32 block_size,
            UInt32 stripe_size,
            UInt32 max_device_write_size,
            IoEngine io_engine,
            UInt32 q_depth_per_context);

        FileDevice(const FileDevice &) = delete;
        FileDevice & operator=(const FileDevice &) = delete;

    private:
        IoContext * getIoContext();

        bool writeImpl(UInt64, UInt32, const void *) override;

        bool readImpl(UInt64, UInt32, void *) override;

        void flushImpl() override;

        const std::vector<File> fvec{};

        const UInt32 stripe_size;

        std::unique_ptr<SyncIoContext> sync_io_contenxt;

        std::atomic<UInt32> incremental_idx{0};

        const IoEngine io_engine;

        const UInt32 q_depth_per_context;

        friend class IoContext;
    };


    class MemoryDevice final : public Device
    {
    public:
        explicit MemoryDevice(UInt64 size, UInt32 io_align_size) : Device{size, io_align_size, 0}, buffer{std::make_unique<UInt8[]>(size)}
        {
        }

        MemoryDevice(const MemoryDevice &) = delete;
        MemoryDevice & operator=(const MemoryDevice &) = delete;
        ~MemoryDevice() override = default;

    private:
        bool writeImpl(UInt64 offset, UInt32 size, const void * value) noexcept override
        {
            chassert(offset + size <= getSize());
            std::memcpy(buffer.get() + offset, value, size);
            return true;
        }

        bool readImpl(UInt64 offset, UInt32 size, void * value) override
        {
            chassert(offset + size <= getSize());
            std::memcpy(value, buffer.get() + offset, size);
            return true;
        }

        void flushImpl() override { }

        std::unique_ptr<UInt8[]> buffer;
    };
}

bool Device::write(UInt64 offset, BufferView value)
{
    const auto value_size = value.size();
    chassert(offset + value_size <= size);
    const UInt8 * data = reinterpret_cast<const UInt8 *>(value.data());
    return writeInternal(offset, data, value_size);
}

bool Device::write(UInt64 offset, Buffer value)
{
    const auto value_size = value.size();
    chassert(offset + value_size <= size);
    UInt8 * data = reinterpret_cast<UInt8 *>(value.data());
    chassert(reinterpret_cast<UInt64>(data) % io_alignment_size == 0ul);
    return writeInternal(offset, data, value_size);
}

bool Device::writeInternal(UInt64 offset, const UInt8 * data, size_t size_)
{
    auto remaining_size = size_;
    auto max_size = (max_write_size == 0) ? remaining_size : max_write_size;
    bool result = true;
    while (remaining_size > 0)
    {
        auto write_size = std::min<size_t>(max_size, remaining_size);
        chassert(offset % io_alignment_size == 0ul);
        chassert(write_size % io_alignment_size == 0ul);

        Stopwatch watch;
        result = writeImpl(offset, write_size, data);
        ProfileEvents::increment(ProfileEvents::DiskCacheDeviceWriteIOLatency, watch.elapsedMicroseconds());

        if (result)
            ProfileEvents::increment(ProfileEvents::DiskCacheDeviceBytesWritten, write_size);
        else
            break;

        offset += write_size;
        data += write_size;
        remaining_size -= write_size;
    }
    if (!result)
        ProfileEvents::increment(ProfileEvents::DiskCacheDeviceWriteIOErrors);

    return result;
}

bool Device::readInternal(UInt64 offset, UInt32 size_, void * data)
{
    chassert(reinterpret_cast<UInt64>(data) % io_alignment_size == 0ul);
    chassert(offset % io_alignment_size == 0ul);
    chassert(size_ % io_alignment_size == 0ul);
    chassert(offset + size_ <= size);

    Stopwatch watch;
    bool result = readImpl(offset, size_, data);
    ProfileEvents::increment(ProfileEvents::DiskCacheDeviceReadIOLatency, watch.elapsedMicroseconds());

    if (!result)
    {
        ProfileEvents::increment(ProfileEvents::DiskCacheDeviceReadIOErrors);
        return result;
    }

    ProfileEvents::increment(ProfileEvents::DiskCacheDeviceBytesRead, size_);
    return true;
}

Buffer Device::read(UInt64 offset, UInt32 size_)
{
    chassert(offset + size_ <= size);
    UInt64 read_offset = offset & ~(static_cast<UInt64>(io_alignment_size) - 1ul);
    UInt64 read_prefix_size = offset & (static_cast<UInt64>(io_alignment_size) - 1ul);
    auto read_size = getIOAlignedSize(read_prefix_size + size_);
    auto buffer = makeIOBuffer(read_size);
    bool result = readInternal(read_offset, read_size, buffer.data());
    if (!result)
        return Buffer{};
    buffer.trimStart(read_prefix_size);
    buffer.shrink(size_);
    return buffer;
}

bool Device::read(UInt64 offset, UInt32 size_, void * value)
{
    return readInternal(offset, size_, value);
}


namespace
{
    std::string IOOp::toString() const
    {
        return fmt::format(
            "[req {}] idx {} fd {} op {} offset {} size {} data {} resubmitted {}",
            reinterpret_cast<const void *>(&parent),
            idx,
            fd,
            parent.getOpName(),
            offset,
            size,
            data,
            static_cast<bool>(resubmitted));
    }

    bool IOOp::done(ssize_t status)
    {
        chassert(parent.op_type == READ || parent.op_type == WRITE);

        bool result = (status == size);
        if (!result)
            Device::logger().error(
                fmt::format("[{}] IO error: {} ret={}", parent.context.getName(), toString(), status, errno, std::strerror(errno)));

        auto cur_time = getSteadyClock();
        auto delay_ms = toMillis(cur_time - start_time).count();
        if (delay_ms > static_cast<Int64>(kIOTimeoutMs))
            Device::logger().error(fmt::format(
                "[{}] IO timeout {}ms (submit +{}ms comp +{}ms): {}",
                parent.context.getName(),
                delay_ms,
                toMillis(submit_time - start_time).count(),
                toMillis(cur_time - submit_time).count(),
                toString()));

        parent.notifyOpResult(result);
        return result;
    }

    IOReq::IOReq(
        IoContext & context_,
        const std::vector<File> & fvec,
        UInt32 stripe_size,
        OpType op_type_,
        UInt64 offset_,
        UInt32 size_,
        void * data_)
        : context(context_), op_type(op_type_), offset(offset_), size(size_), data(data_)
    {
        UInt8 * buf = reinterpret_cast<UInt8 *>(data);
        UInt32 idx = 0;
        if (fvec.size() > 1)
        {
            // RAID device
            while (size_ > 0)
            {
                UInt64 stripe = offset_ / stripe_size;
                UInt32 fd_idx = stripe % fvec.size();
                UInt64 stripe_start_offset = (stripe / fvec.size()) * stripe_size;
                UInt32 io_offset_in_stripe = offset_ % stripe_size;
                UInt32 allowed_io_size = std::min(size_, stripe_size - io_offset_in_stripe);

                ops.emplace_back(*this, idx++, fvec[fd_idx].getFd(), stripe_start_offset + io_offset_in_stripe, allowed_io_size, buf);

                size_ -= allowed_io_size;
                offset_ += allowed_io_size;
                buf += allowed_io_size;
            }
        }
        else
            ops.emplace_back(*this, idx++, fvec[0].getFd(), offset, size, data);

        num_remaining = ops.size();
    }

    bool IOReq::waitCompletion()
    {
        if (context.isAsyncIoCompletion())
            // not supported at now()
            throwFromErrno("not supported", ErrorCodes::NOT_IMPLEMENTED);

        auto cur_time = getSteadyClock();

        Int64 delay_ms = 0;
        if (ops.size() > 1)
            delay_ms = toMillis(cur_time - start_time).count();
        else
            delay_ms = toMillis(cur_time - comp_time).count();

        if (delay_ms > static_cast<Int64>(kIOTimeoutMs))
            Device::logger().error(fmt::format(
                "[{}] IOReq timeout {}ms (comp +{}ms notify +{}ms): {}",
                context.getName(),
                delay_ms,
                toMillis(comp_time - start_time).count(),
                toMillis(cur_time - comp_time).count(),
                toString()));

        return result;
    }

    void IOReq::notifyOpResult(bool result_)
    {
        result = result && result_;
        chassert(num_remaining > 0u);
        if (--num_remaining > 0)
            return;

        comp_time = getSteadyClock();
        if (context.isAsyncIoCompletion())
            throwFromErrno("not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    std::shared_ptr<IOReq>
    IoContext::submitRead(const std::vector<File> & fvec, UInt32 stripe_size, UInt64 offset, UInt32 size, void * data)
    {
        auto req = std::make_shared<IOReq>(*this, fvec, stripe_size, OpType::READ, offset, size, data);
        submitReq(req);
        return req;
    }

    std::shared_ptr<IOReq>
    IoContext::submitWrite(const std::vector<File> & fvec, UInt32 stripe_size, UInt64 offset, UInt32 size, const void * data)
    {
        auto req = std::make_shared<IOReq>(*this, fvec, stripe_size, OpType::WRITE, offset, size, const_cast<void *>(data));
        submitReq(req);
        return req;
    }

    void IoContext::submitReq(std::shared_ptr<IOReq> req)
    {
        req->start_time = getSteadyClock();
        for (auto & op : req->ops)
        {
            if (!submitIo(op))
            {
                chassert(!isAsyncIoCompletion());
                break;
            }
        }
    }

    ssize_t SyncIoContext::writeSync(int fd, UInt64 offset, UInt32 size, const void * value) { return ::pwrite(fd, value, size, offset); }

    ssize_t SyncIoContext::readSync(int fd, UInt64 offset, UInt32 size, void * value) { return ::pread(fd, value, size, offset); }

    bool SyncIoContext::submitIo(IOOp & op)
    {
        op.start_time = getSteadyClock();

        ssize_t status;
        if (op.parent.op_type == OpType::READ)
            status = readSync(op.fd, op.offset, op.size, op.data);
        else
        {
            chassert(op.parent.op_type == OpType::WRITE);
            status = writeSync(op.fd, op.offset, op.size, op.data);
        }

        op.submit_time = getSteadyClock();

        return op.done(status);
    }

    FileDevice::FileDevice(
        std::vector<DB::File> && fvec_,
        UInt64 file_size,
        UInt32 block_size,
        UInt32 stripe_size_,
        UInt32 max_device_write_size,
        IoEngine io_engine_,
        UInt32 q_depth_per_context_)
        : Device(file_size * fvec_.size(), block_size, max_device_write_size)
        , fvec(std::move(fvec_))
        , stripe_size(stripe_size_)
        , io_engine(io_engine_)
        , q_depth_per_context(q_depth_per_context_)
    {
        chassert(block_size > 0u);
        if (fvec.size() > 1)
        {
            chassert(stripe_size > 0u);
            chassert(stripe_size >= block_size);
            chassert(0u == stripe_size % 2);
            chassert(0u == stripe_size % block_size);

            if (file_size % stripe_size != 0)
                throwFromErrno(
                    fmt::format(
                        "Invalid size because individual device size: {} is not aligned to stripe size: {}", file_size, stripe_size),
                    ErrorCodes::BAD_ARGUMENTS);
        }

        if (io_engine == IoEngine::Sync)
        {
            chassert(q_depth_per_context == 0u);
            sync_io_contenxt = std::make_unique<SyncIoContext>();
        }
        else
        {
            throwFromErrno("not supported", ErrorCodes::NOT_IMPLEMENTED);
        }

        Device::logger().information(fmt::format(
            "Created device with num_devices {} size {} block_size {} stripe_size {} max_write_size {} io_engine {} qdepth {}",
            fvec.size(),
            getSize(),
            block_size,
            stripe_size,
            max_device_write_size,
            getIoEngineName(io_engine),
            q_depth_per_context));
    }

    bool FileDevice::readImpl(UInt64 offset, UInt32 size, void * value)
    {
        auto req = getIoContext()->submitRead(fvec, stripe_size, offset, size, value);
        return req->waitCompletion();
    }

    bool FileDevice::writeImpl(UInt64 offset, UInt32 size, const void * value)
    {
        auto req = getIoContext()->submitWrite(fvec, stripe_size, offset, size, value);
        return req->waitCompletion();
    }

    void FileDevice::flushImpl()
    {
        for (const auto & f : fvec)
            ::fsync(f.getFd());
    }

    IoContext * FileDevice::getIoContext()
    {
        if (io_engine == IoEngine::Sync)
            return sync_io_contenxt.get();

        throwFromErrno("not supported", ErrorCodes::NOT_IMPLEMENTED);
    }
}

std::unique_ptr<Device> createMemoryDevice(UInt64 size, UInt32 io_align_size)
{
    return std::make_unique<MemoryDevice>(size, io_align_size);
}

std::unique_ptr<Device> createDirectIoFileDevice(
    std::vector<DB::File> f_vec,
    UInt64 file_size,
    UInt32 block_size,
    UInt32 stripe_size,
    UInt32 max_device_write_size,
    IoEngine io_engine,
    UInt32 q_depth)
{
    chassert(isPowerOf2(block_size));

    return std::make_unique<FileDevice>(std::move(f_vec), file_size, block_size, stripe_size, max_device_write_size, io_engine, q_depth);
}

std::unique_ptr<Device>
createDirectIoFileDevice(std::vector<DB::File> f_vec, UInt64 file_size, UInt32 block_size, UInt32 stripe_size, UInt32 max_device_write_size)
{
    return createDirectIoFileDevice(std::move(f_vec), file_size, block_size, stripe_size, max_device_write_size, IoEngine::Sync, 0);
}
}
