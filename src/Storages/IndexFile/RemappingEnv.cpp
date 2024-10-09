#include <Storages/IndexFile/RemappingEnv.h>
#include <exception>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string.h>
#include <common/types.h>
#include <common/defines.h>
#include <Core/Defines.h>
#include <Common/Logger.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <Storages/IndexFile/Env.h>
#include <Storages/IndexFile/Status.h>

namespace DB::IndexFile
{

/// WriteRemappingEnv should have longer life time than RemappingWritableFile
class RemappingWritableFile: public WritableFile
{
public:
    RemappingWritableFile(const String& file_name_, WriteRemappingEnv& env_,
        const std::function<void()>& destroy_callback_):
            base_offset(env_.buffer->count()), file_name(file_name_),
            destroy_callback(destroy_callback_), env(env_), closed(false),
            written_size(0)
    {
    }

    virtual ~RemappingWritableFile() override
    {
        if (!closed)
        {
            Status status = Close();
            LOG_ERROR(getLogger("RemappingWriteableFile"), "File {} not closed "
                "before dtor, trying to close it automatically {}", file_name,
                status.ToString());
        }

        destroy_callback();
    }

    virtual Status Append(const Slice& data) override
    {
        try
        {
            assertNotClosed();

            env.buffer->write(data.data(), data.size());
            written_size += data.size();
            return Status::OK();
        }
        catch (const std::exception& e)
        {
            return Status::IOError(fmt::format("File {} append error {}",
                env.buffer->getFileName(), e.what()));
        }
    }

    virtual Status Close() override
    {
        closed = true;

        if (!env.emplaceFile(file_name, base_offset, written_size))
        {
            return Status::InvalidArgument(fmt::format("File {} already exist in "
                "WriteRemappingEnv", file_name));
        }

        return Status::OK();
    }

    virtual Status Flush() override
    {
        try
        {
            assertNotClosed();

            env.buffer->next();
            return Status::OK();
        }
        catch (const std::exception& e)
        {
            return Status::IOError(fmt::format("File {} flush error {}",
                env.buffer->getFileName(), e.what()));
        }
    }

    virtual Status Sync([[maybe_unused]]bool need_fsync) override
    {
        try
        {
            assertNotClosed();

            env.buffer->next();
            if (need_fsync)
            {
                env.buffer->sync();
            }
            return Status::OK();
        }
        catch (const std::exception& e)
        {
            return Status::IOError(fmt::format("File {} sync error {}",
                env.buffer->getFileName(), e.what()));
        }
    }

private:
    inline void assertNotClosed() const
    {
        if (unlikely(closed))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying write to a closed file {}",
                file_name);
        }
    }

    const UInt64 base_offset;
    const String file_name;
    const std::function<void()> destroy_callback;

    WriteRemappingEnv& env;

    bool closed;
    UInt64 written_size;
};

WriteRemappingEnv::WriteRemappingEnv(std::unique_ptr<WriteBufferFromFileBase>&& buffer_):
    buffer(std::move(buffer_)), finalized(false), active_write_files(0)
{
}

Status WriteRemappingEnv::NewRandomAccessFile(const std::string&,
    std::unique_ptr<RandomAccessFile>*)
{
    return Status::NotSupported("NewRandomAccessFile is not supported "
        "by WriteRemappingEnv");
}

Status WriteRemappingEnv::NewRandomAccessRemoteFileWithCache(const RemoteFileInfo&,
    RemoteFileCachePtr, std::unique_ptr<RandomAccessFile>*)
{
    return Status::NotSupported("NewRandomAccessRemoteFileWithCache is not supported "
        "by WriteRemappingEnv");
}

Status WriteRemappingEnv::NewWritableFile(const std::string& fname,
    std::unique_ptr<WritableFile>* result)
{
    std::lock_guard lock(mu);

    if (Status s = noActiveFilesAndNotFinalized(); !s.ok())
    {
        return s;
    }

    if (file_metas.contains(fname))
    {
        return Status::InvalidArgument(fmt::format("File {} already exists", fname));
    }

    ++active_write_files;
    *result = std::make_unique<RemappingWritableFile>(fname, *this,
        [this]() {
            std::lock_guard file_lock(mu);
            --active_write_files;
        }
    );
    return Status::OK();
}

bool WriteRemappingEnv::FileExists(const std::string& fname)
{
    std::lock_guard lock(mu);
    return file_metas.contains(fname);
}

Status WriteRemappingEnv::GetFileSize(const std::string& fname, uint64_t* file_size)
{
    std::lock_guard lock(mu);
    if (auto iter = file_metas.find(fname); iter != file_metas.end())
    {
        *file_size = iter->second.second;
        return Status::OK();
    }
    return Status::NotFound(fmt::format("File {} not found in WriteRemappingEnv",
        fname));
}

Status WriteRemappingEnv::RenameFile(const std::string&, const std::string&)
{
    return Status::NotSupported("RenameFile is not supported by WriteRemappingEnv");
}

Status WriteRemappingEnv::DeleteFile(const std::string&)
{
    return Status::NotSupported("DeleteFile is not supported by WriteRemappingEnv");
}

Status WriteRemappingEnv::finalize()
{
    std::lock_guard lock(mu);
    if (Status s = noActiveFilesAndNotFinalized(); !s.ok())
    {
        return s;
    }

    try
    {
        finalized = true;

        UInt64 meta_start_offset = buffer->count();
        writeIntBinary(static_cast<UInt64>(file_metas.size()), *buffer);
        for (const auto& entry : file_metas)
        {
            writeBinary(entry.first, *buffer);
            writeBinary(entry.second, *buffer);
        }
        writeIntBinary(meta_start_offset, *buffer);

        /// Release buffer here, no longer accept any requests
        buffer->finalize();
        buffer = nullptr;

        return Status::OK();
    }
    catch (std::exception& e)
    {
        return Status::IOError(fmt::format("Failed to finalize WriteRemappingEnv to "
            "file {}, error {}", buffer->getFileName(), e.what()));
    }
}

Status WriteRemappingEnv::underlyingFileOffset(size_t* offset_)
{
    std::lock_guard lock(mu);
    if (unlikely(active_write_files != 0))
    {
        return Status::InvalidArgument("WriteRemappingEnv still have ongoing write");
    }
    *offset_ = buffer->count();
    return Status::OK();
}

Status WriteRemappingEnv::noActiveFilesAndNotFinalized() const
{
    if (unlikely(active_write_files != 0))
    {
        return Status::InvalidArgument("WriteRemappingEnv still have ongoing write");
    }
    if (unlikely(finalized))
    {
        return Status::InvalidArgument("WriteRemappingEnv is already finalized");
    }
    return Status::OK();
}

bool WriteRemappingEnv::emplaceFile(const String& file_name_, UInt64 offset_, UInt64 size_)
{
    std::lock_guard lock(mu);
    return file_metas.emplace(file_name_, std::make_pair(offset_, size_)).second;
}

class ConcurrentFileReader
{
public:
    explicit ConcurrentFileReader(ReadRemappingEnv::FileBufferCreator& buffer_creator_):
        buffer_creator(buffer_creator_)
    {
    }

    Status read(uint64_t offset, size_t n, char* buffer, Slice* result)
    {
        /// NOTE: We can cache first ReadBufferFromFileDescriptor, but this will hold
        /// file handler indefinitely, and bypass DiskCache's eviction
        try
        {
            std::unique_ptr<SeekableReadBuffer> reader = buffer_creator(n);

            size_t readed = 0;
            if (reader->supportsReadAt())
            {
                readed = reader->readBigAt(buffer, n, offset);
            }
            else
            {
                reader->seek(offset);
                readed = reader->read(buffer, n);
            }
            *result = Slice(buffer, readed);
            return Status::OK();
        }
        catch (const std::exception& e)
        {
            return Status::IOError(fmt::format("Failed to read from ConcurrentFileReader, error {}",
                e.what()));
        }
    }

private:
    ReadRemappingEnv::FileBufferCreator& buffer_creator;
};

class RemappingRandomAccessFile: public RandomAccessFile
{
public:
    RemappingRandomAccessFile(ReadRemappingEnv::FileBufferCreator& buffer_creator_,
        UInt64 offset_, UInt64 size_):
            base_offset(offset_), file_size(size_), file_reader(buffer_creator_)
    {
    }

    virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch,
        bool* from_local) const override
    {
        if (from_local)
        {
            *from_local = true;
        }

        if (offset >= file_size)
        {
            *result = Slice(scratch, 0);
            return Status::OK();
        }
        else
        {
            size_t bytes_to_read = std::min(n, file_size - offset);
            return file_reader.read(base_offset + offset, bytes_to_read, scratch,
                result);
        }
    }

private:
    const UInt64 base_offset;
    const UInt64 file_size;
    mutable ConcurrentFileReader file_reader;
};

ReadRemappingEnv::ReadRemappingEnv(const FileBufferCreator& buffer_creator_,
    UInt64 file_end_offset_): buffer_creator(buffer_creator_)
{
    std::unique_ptr<SeekableReadBuffer> buffer = buffer_creator(DBMS_DEFAULT_BUFFER_SIZE);
    
    /// Preload data. A 100GB part with 256 digestion size will have 400 files,
    /// assume each file have 1KB meta(file name and meta), 1MB is enough for these
    /// files, so we seek to file_end - 1M position and read entire file meta in one IO
    {
        size_t preload_start_offset = 0;
        if (file_end_offset_ > (DBMS_DEFAULT_BUFFER_SIZE - 1))
        {
            preload_start_offset = file_end_offset_ - (DBMS_DEFAULT_BUFFER_SIZE - 1);
        }
        buffer->seek(preload_start_offset);
        buffer->eof();
    }

    /// Read meta start offset
    buffer->seek(file_end_offset_ - sizeof(UInt64), SEEK_SET);
    UInt64 file_meta_offset = 0;
    readIntBinary(file_meta_offset, *buffer);

    /// Read file meta count
    buffer->seek(file_meta_offset, SEEK_SET);
    UInt64 file_meta_count = 0;
    readIntBinary(file_meta_count, *buffer);

    /// Read file meta
    String file_name;
    std::pair<UInt64, UInt64> offset_and_size;
    for (size_t i = 0; i < file_meta_count; ++i)
    {
        readBinary(file_name, *buffer);
        readBinary(offset_and_size, *buffer);

        file_metas.emplace(file_name, offset_and_size);
    }
}

Status ReadRemappingEnv::NewRandomAccessFile(const std::string& fname,
    std::unique_ptr<RandomAccessFile>* result)
{
    if (auto iter = file_metas.find(fname); iter != file_metas.end())
    {
        *result = std::make_unique<RemappingRandomAccessFile>(buffer_creator,
            iter->second.first, iter->second.second);
        return Status::OK();
    }
    return Status::NotFound(fmt::format("File {} not found in ReadReamppingEnv",
        fname));
}

Status ReadRemappingEnv::NewRandomAccessRemoteFileWithCache(const RemoteFileInfo&,
    RemoteFileCachePtr, std::unique_ptr<RandomAccessFile>*)
{
    return Status::NotSupported("NewRandomAccessRemoteFileWithCache is not supported "
        "by ReadRemappingEnv");
}

Status ReadRemappingEnv::NewWritableFile(const std::string&,
    std::unique_ptr<WritableFile>*)
{
    return Status::NotSupported("NewWritableFile is not supported "
        "by ReadRemappingEnv");
}

bool ReadRemappingEnv::FileExists(const std::string& fname)
{
    return file_metas.contains(fname);
}

Status ReadRemappingEnv::GetFileSize(const std::string& fname, uint64_t* file_size)
{
    if (auto iter = file_metas.find(fname); iter != file_metas.end())
    {
        *file_size = iter->second.second;
        return Status::OK();
    }
    return Status::NotFound(fmt::format("File {} not found in ReadRemappingEnv",
        fname));
}

Status ReadRemappingEnv::RenameFile(const std::string&, const std::string&)
{
    return Status::NotSupported("RenameFile is not supported "
        "by ReadRemappingEnv");
}

Status ReadRemappingEnv::DeleteFile(const std::string&)
{
    return Status::NotSupported("DeleteFile is not supported "
        "by ReadRemappingEnv");
}

}
