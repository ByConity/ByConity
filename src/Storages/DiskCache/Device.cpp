#include <cstddef>
#include <cstring>
#include <filesystem>
#include <memory>
#include <vector>
#include <unistd.h>

#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/File.h>
#include <Storages/DiskCache/TimeUtil.h>
#include <Storages/DiskCache/Types.h>
#include <fmt/core.h>
#include <sys/types.h>
#include <Poco/AtomicCounter.h>
#include <Poco/Logger.h>
#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
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

Device::Device(UInt64 size, UInt32 io_align_size, UInt32 max_write_size)
    : size_(size), io_alignment_size_(io_align_size), max_write_size_(max_write_size)
{
    if (io_align_size == 0)
        throwFromErrno(fmt::format("Invalid io_align_size: {}", io_align_size), ErrorCodes::BAD_ARGUMENTS);

    if (max_write_size_ % io_alignment_size_ != 0)
        throwFromErrno(
            fmt::format("Invalid max_write_size: {}, io_align_size: {}", max_write_size_, io_alignment_size_), ErrorCodes::BAD_ARGUMENTS);

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
        explicit IOOp(IOReq & parent, int idx, int fd, UInt64 offset, UInt32 size, void * data)
            : parent_(parent), idx_(idx), fd_(fd), offset_(offset), size_(size), data_(data)
        {
        }

        std::string toString() const;

        bool done(ssize_t status);

        IOReq & parent_;

        const UInt32 idx_;

        const int fd_;
        const UInt64 offset_ = 0;
        const UInt32 size_ = 0;
        void * const data_;

        UInt8 resubmitted_ = 0;

        std::chrono::nanoseconds start_time_;
        std::chrono::nanoseconds submit_time_;
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
            switch (op_type_)
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
                offset_,
                size_,
                data_,
                ops_.size(),
                num_remaining_,
                result_);
        }

        IoContext & context_;
        const OpType op_type_ = OpType::INVALID;
        const UInt64 offset_ = 0;
        const UInt32 size_ = 0;
        void * const data_;

        bool result_ = true;

        UInt32 num_remaining_ = 0;
        std::vector<IOOp> ops_;

        std::chrono::nanoseconds start_time_;
        std::chrono::nanoseconds comp_time_;
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
            std::vector<File> && fvec,
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

        const std::vector<File> fvec_{};

        const UInt32 stripe_size_;

        std::unique_ptr<SyncIoContext> sync_io_contenxt_;

        std::atomic<UInt32> incremental_idx_{0};

        const IoEngine io_engine_;

        const UInt32 q_depth_per_context_;

        Poco::AtomicCounter num_processed_{0};

        friend class IoContext;
    };


    class MemoryDevice final : public Device
    {
    public:
        explicit MemoryDevice(UInt64 size, UInt32 io_align_size) : Device{size, io_align_size, 0}, buffer_{std::make_unique<UInt8[]>(size)}
        {
        }

        MemoryDevice(const MemoryDevice &) = delete;
        MemoryDevice & operator=(const MemoryDevice &) = delete;
        ~MemoryDevice() override = default;

    private:
        bool writeImpl(UInt64 offset, UInt32 size, const void * value) noexcept override
        {
            chassert(offset + size <= getSize());
            std::memcpy(buffer_.get() + offset, value, size);
            return true;
        }

        bool readImpl(UInt64 offset, UInt32 size, void * value) override
        {
            chassert(offset + size <= getSize());
            std::memcpy(value, buffer_.get() + offset, size);
            return true;
        }

        void flushImpl() override { }

        std::unique_ptr<UInt8[]> buffer_;
    };
}

bool Device::write(UInt64 offset, BufferView value)
{
    const auto size = value.size();
    chassert(offset + size <= size_);
    const UInt8 * data = reinterpret_cast<const UInt8 *>(value.data());
    return writeInternal(offset, data, size);
}

bool Device::write(UInt64 offset, Buffer value)
{
    const auto size = value.size();
    chassert(offset + size <= size_);
    UInt8 * data = reinterpret_cast<UInt8 *>(value.data());
    chassert(reinterpret_cast<UInt64>(data) % io_alignment_size_ == 0ul);
    return writeInternal(offset, data, size);
}

bool Device::writeInternal(UInt64 offset, const UInt8 * data, size_t size)
{
    auto remaining_size = size;
    auto max_write_size = (max_write_size_ == 0) ? remaining_size : max_write_size_;
    bool result = true;
    while (remaining_size > 0)
    {
        auto write_size = std::min<size_t>(max_write_size, remaining_size);
        chassert(offset % io_alignment_size_ == 0ul);
        chassert(write_size % io_alignment_size_ == 0ul);

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

bool Device::readInternal(UInt64 offset, UInt32 size, void * data)
{
    chassert(reinterpret_cast<UInt64>(data) % io_alignment_size_ == 0ul);
    chassert(offset % io_alignment_size_ == 0ul);
    chassert(size % io_alignment_size_ == 0ul);
    chassert(offset + size <= size_);

    Stopwatch watch;
    bool result = readImpl(offset, size, data);
    ProfileEvents::increment(ProfileEvents::DiskCacheDeviceReadIOLatency, watch.elapsedMicroseconds());

    if (!result)
    {
        ProfileEvents::increment(ProfileEvents::DiskCacheDeviceReadIOErrors);
        return result;
    }

    ProfileEvents::increment(ProfileEvents::DiskCacheDeviceBytesRead, size);
    return true;
}

Buffer Device::read(UInt64 offset, UInt32 size)
{
    chassert(offset + size <= size_);
    UInt64 read_offset = offset & ~(static_cast<UInt64>(io_alignment_size_) - 1ul);
    UInt64 read_prefix_size = offset & (static_cast<UInt64>(io_alignment_size_) - 1ul);
    auto read_size = getIOAlignedSize(read_prefix_size + size);
    auto buffer = makeIOBuffer(read_size);
    bool result = readInternal(read_offset, size, buffer.data());
    if (!result)
        return Buffer{};
    buffer.trimStart(read_prefix_size);
    buffer.shrink(size);
    return buffer;
}

bool Device::read(UInt64 offset, UInt32 size, void * value)
{
    return readInternal(offset, size, value);
}


namespace
{
    std::string IOOp::toString() const
    {
        return fmt::format(
            "[req {}] idx {} fd {} op {} offset {} size {} data {} resubmitted {}",
            reinterpret_cast<const void *>(&parent_),
            idx_,
            fd_,
            parent_.getOpName(),
            offset_,
            size_,
            data_,
            static_cast<bool>(resubmitted_));
    }

    bool IOOp::done(ssize_t status)
    {
        chassert(parent_.op_type_ == READ || parent_.op_type_ == WRITE);

        bool result = (status == size_);
        if (!result)
            Device::logger().error(
                fmt::format("[{}] IO error: {} ret={}", parent_.context_.getName(), toString(), status, errno, std::strerror(errno)));

        auto cur_time = getSteadyClock();
        auto delay_ms = toMillis(cur_time - start_time_).count();
        if (delay_ms > static_cast<Int64>(kIOTimeoutMs))
            Device::logger().error(fmt::format(
                "[{}] IO timeout {}ms (submit +{}ms comp +{}ms): {}",
                parent_.context_.getName(),
                delay_ms,
                toMillis(submit_time_ - start_time_).count(),
                toMillis(cur_time - submit_time_).count(),
                toString()));

        parent_.notifyOpResult(result);
        return result;
    }

    IOReq::IOReq(
        IoContext & context, const std::vector<File> & fvec, UInt32 stripe_size, OpType op_type, UInt64 offset, UInt32 size, void * data)
        : context_(context), op_type_(op_type), offset_(offset), size_(size), data_(data)
    {
        UInt8 * buf = reinterpret_cast<UInt8 *>(data_);
        UInt32 idx = 0;
        if (fvec.size() > 1)
        {
            // RAID device
            while (size > 0)
            {
                UInt64 stripe = offset / stripe_size;
                UInt32 fd_idx = stripe % fvec.size();
                UInt64 stripe_start_offset = (stripe / fvec.size()) * stripe_size;
                UInt32 io_offset_in_stripe = offset % stripe_size;
                UInt32 allowed_io_size = std::min(size, stripe_size - io_offset_in_stripe);

                ops_.emplace_back(*this, idx++, fvec[fd_idx].fd(), stripe_start_offset + io_offset_in_stripe, allowed_io_size, buf);

                size -= allowed_io_size;
                offset += allowed_io_size;
                buf += allowed_io_size;
            }
        }
        else
            ops_.emplace_back(*this, idx++, fvec[0].fd(), offset_, size_, data_);

        num_remaining_ = ops_.size();
    }

    bool IOReq::waitCompletion()
    {
        if (context_.isAsyncIoCompletion())
            // not supported at now()
            throwFromErrno("not supported", ErrorCodes::NOT_IMPLEMENTED);

        auto cur_time = getSteadyClock();

        Int64 delay_ms = 0;
        if (ops_.size() > 1)
            delay_ms = toMillis(cur_time - start_time_).count();
        else
            delay_ms = toMillis(cur_time - comp_time_).count();

        if (delay_ms > static_cast<Int64>(kIOTimeoutMs))
            Device::logger().error(fmt::format(
                "[{}] IOReq timeout {}ms (comp +{}ms notify +{}ms): {}",
                context_.getName(),
                delay_ms,
                toMillis(comp_time_ - start_time_).count(),
                toMillis(cur_time - comp_time_).count(),
                toString()));

        return result_;
    }

    void IOReq::notifyOpResult(bool result)
    {
        result_ = result_ && result;
        chassert(num_remaining_ > 0u);
        if (--num_remaining_ > 0)
            return;

        comp_time_ = getSteadyClock();
        if (context_.isAsyncIoCompletion())
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
        req->start_time_ = getSteadyClock();
        for (auto & op : req->ops_)
        {
            if (!submitIo(op))
            {
                chassert(!isAsyncIoCompletion());
                break;
            }
        }
    }

    ssize_t SyncIoContext::writeSync(int fd, UInt64 offset, UInt32 size, const void * value)
    {
        return ::pwrite(fd, value, size, offset);
    }

    ssize_t SyncIoContext::readSync(int fd, UInt64 offset, UInt32 size, void * value)
    {
        return ::pread(fd, value, size, offset);
    }

    bool SyncIoContext::submitIo(IOOp & op)
    {
        op.start_time_ = getSteadyClock();

        ssize_t status;
        if (op.parent_.op_type_ == OpType::READ)
            status = readSync(op.fd_, op.offset_, op.size_, op.data_);
        else
        {
            chassert(op.parent_.op_type_ == OpType::WRITE);
            status = writeSync(op.fd_, op.offset_, op.size_, op.data_);
        }

        op.submit_time_ = getSteadyClock();

        return op.done(status);
    }

    FileDevice::FileDevice(
        std::vector<File> && fvec,
        UInt64 file_size,
        UInt32 block_size,
        UInt32 stripe_size,
        UInt32 max_device_write_size,
        IoEngine io_engine,
        UInt32 q_depth_per_context)
        : Device(file_size * fvec.size(), block_size, max_device_write_size)
        , fvec_(std::move(fvec))
        , stripe_size_(stripe_size)
        , io_engine_(io_engine)
        , q_depth_per_context_(q_depth_per_context)
    {
        chassert(block_size > 0u);
        if (fvec_.size() > 1)
        {
            chassert(stripe_size_ > 0u);
            chassert(stripe_size_ >= block_size);
            chassert(0u == stripe_size_ % 2);
            chassert(0u == stripe_size_ % block_size);

            if (file_size % stripe_size != 0)
                throwFromErrno(
                    fmt::format(
                        "Invalid size because individual device size: {} is not aligned to stripe size: {}", file_size, stripe_size),
                    ErrorCodes::BAD_ARGUMENTS);
        }

        if (io_engine == IoEngine::Sync)
        {
            chassert(q_depth_per_context_ == 0u);
            sync_io_contenxt_ = std::make_unique<SyncIoContext>();
        }
        else
        {
            throwFromErrno("not supported", ErrorCodes::NOT_IMPLEMENTED);
        }

        Device::logger().information(fmt::format(
            "Created device with num_devices {} size {} block_size {} stripe_size {} max_write_size {} io_engine {} qdepth {}",
            fvec_.size(),
            getSize(),
            block_size,
            stripe_size,
            max_device_write_size,
            getIoEngineName(io_engine_),
            q_depth_per_context_));
    }

    bool FileDevice::readImpl(UInt64 offset, UInt32 size, void * value)
    {
        auto req = getIoContext()->submitRead(fvec_, stripe_size_, offset, size, value);
        return req->waitCompletion();
    }

    bool FileDevice::writeImpl(UInt64 offset, UInt32 size, const void * value)
    {
        auto req = getIoContext()->submitWrite(fvec_, stripe_size_, offset, size, value);
        return req->waitCompletion();
    }

    void FileDevice::flushImpl()
    {
        for (const auto & f : fvec_)
            ::fsync(f.fd());
    }

    IoContext * FileDevice::getIoContext()
    {
        if (io_engine_ == IoEngine::Sync)
            return sync_io_contenxt_.get();

        throwFromErrno("not supported", ErrorCodes::NOT_IMPLEMENTED);
    }
}

std::unique_ptr<Device> createMemoryDevice(UInt64 size, UInt32 io_align_size)
{
    return std::make_unique<MemoryDevice>(size, io_align_size);
}

std::unique_ptr<Device> createDirectIoFileDevice(
    std::vector<File> f_vec,
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
createDirectIoFileDevice(std::vector<File> f_vec, UInt64 file_size, UInt32 block_size, UInt32 stripe_size, UInt32 max_device_write_size)
{
    return createDirectIoFileDevice(std::move(f_vec), file_size, block_size, stripe_size, max_device_write_size, IoEngine::Sync, 0);
}
}
