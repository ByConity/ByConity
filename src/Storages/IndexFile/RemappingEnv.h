#pragma once

#include <unordered_map>
#include <common/types.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <Storages/IndexFile/Env.h>

namespace DB::IndexFile
{

/// Allow write multiple files' data to same file system file, only
/// one file is allowed to open at same time, open file for read,
/// get file size, rename file, delete file is forbidden
/// File format is | file data | file data | ... | file metas | meta_offset |
class WriteRemappingEnv: public Env
{
public:
    explicit WriteRemappingEnv(std::unique_ptr<WriteBufferFromFileBase>&& buffer_);

    virtual Status NewRandomAccessFile(const std::string& fname,
        std::unique_ptr<RandomAccessFile>* result) override;
    virtual Status NewRandomAccessRemoteFileWithCache(const RemoteFileInfo& file,
        RemoteFileCachePtr cache, std::unique_ptr<RandomAccessFile>* result) override;
    virtual Status NewWritableFile(const std::string& fname,
        std::unique_ptr<WritableFile>* result) override;
    virtual bool FileExists(const std::string& fname) override;
    virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) override;
    virtual Status RenameFile(const std::string& src, const std::string& target) override;
    virtual Status DeleteFile(const std::string& fname) override;

    /// Close underlying file and forbidden incoming write
    Status finalize();

    /// Get current underlying file offset
    Status underlyingFileOffset(size_t* offset_);

private:
    friend class RemappingWritableFile;

    inline Status noActiveFilesAndNotFinalized() const;
    bool emplaceFile(const String& file_name_, UInt64 offset_, UInt64 size_);

    /// All operation to this env must acquire this lock, to meet thread
    /// safety request for Env implementation
    std::mutex mu;

    std::unique_ptr<WriteBufferFromFileBase> buffer;

    bool finalized;
    size_t active_write_files;
    /// First value in pair is file offset, second value in pair is file size
    std::unordered_map<String, std::pair<UInt64, UInt64>> file_metas;
};

class ReadRemappingEnv: public Env
{
public:
    using FileBufferCreator = std::function<std::unique_ptr<SeekableReadBuffer>(size_t)>;

    /// NOTE: FileBufferCreator may returns different type of ReadBuffer between call
    /// to call, e.g. It returns s3 read buffer when file not cached to local
    ReadRemappingEnv(const FileBufferCreator& buffer_creator_, UInt64 file_end_offset_);

    virtual Status NewRandomAccessFile(const std::string& fname,
        std::unique_ptr<RandomAccessFile>* result) override;
    virtual Status NewRandomAccessRemoteFileWithCache(const RemoteFileInfo& file,
        RemoteFileCachePtr cache, std::unique_ptr<RandomAccessFile>* result) override;
    virtual Status NewWritableFile(const std::string& fname,
        std::unique_ptr<WritableFile>* result) override;
    virtual bool FileExists(const std::string& fname) override;
    virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) override;
    virtual Status RenameFile(const std::string& src, const std::string& target) override;
    virtual Status DeleteFile(const std::string& fname) override;

private:
    friend class RemappingRandomAccessFile;

    FileBufferCreator buffer_creator;
    std::unordered_map<String, std::pair<UInt64, UInt64>> file_metas;
};

}
