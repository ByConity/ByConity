/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <Interpreters/Context_fwd.h>
#include <Core/Defines.h>
#include <common/types.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include "Core/Field.h"
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <Disks/Executor.h>
#include <Disks/DiskType.h>

#include <memory>
#include <mutex>
#include <utility>
#include <boost/noncopyable.hpp>
#include <fmt/core.h>
#include <Poco/Timestamp.h>
#include <filesystem>
#include "Poco/Util/AbstractConfiguration.h"

namespace fs = std::filesystem;

namespace CurrentMetrics
{
extern const Metric DiskSpaceReservedForMerge;
}

namespace DB
{

extern std::atomic<UInt64> next_disk_id;

class IDiskDirectoryIterator;
using DiskDirectoryIteratorPtr = std::unique_ptr<IDiskDirectoryIterator>;

class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;
using Reservations = std::vector<ReservationPtr>;

class ReadBufferFromFileBase;
class WriteBufferFromFileBase;
class MMappedFileCache;

/**
 * Provide interface for reservation.
 */
class Space : public std::enable_shared_from_this<Space>
{
public:
    /// Return the name of the space object.
    virtual const String & getName() const = 0;

    /// Reserve the specified number of bytes.
    virtual ReservationPtr reserve(UInt64 bytes) = 0;

    virtual ~Space() = default;
};

using SpacePtr = std::shared_ptr<Space>;

/**
 * A guard, that should synchronize file's or directory's state
 * with storage device (e.g. fsync in POSIX) in its destructor.
 */
class ISyncGuard
{
public:
    ISyncGuard() = default;
    virtual ~ISyncGuard() = default;
};

using SyncGuardPtr = std::unique_ptr<ISyncGuard>;

/**
 * A unit of storage persisting data and metadata.
 * Abstract underlying storage technology.
 * Responsible for:
 * - file management;
 * - space accounting and reservation.
 */

 struct DiskStats
 {
    UInt64 bytes{0};
    UInt64 inodes{0};

    String toString() const
    {
        return fmt::format("DiskStats(bytes={},inodes={})", bytes, inodes);
    }

    void assertStats(const DiskStats & stats) const
    {
        if (bytes < stats.bytes || inodes < stats.inodes)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Current stats {} not allowed less stats {}", toString(), stats.toString());
    }

    DiskStats operator-(const DiskStats & stats) const
    {
        assertStats(stats);
        return {bytes - stats.bytes, inodes - stats.inodes};
    }

    DiskStats operator-=(const DiskStats & stats)
    {
        assertStats(stats);
        bytes -= stats.bytes;
        inodes -= stats.inodes;

        return *this;
    }

    DiskStats operator+(const DiskStats & stats) const
    {
        return {bytes + stats.bytes, inodes + stats.inodes};
    }

    DiskStats operator+=(const DiskStats & stats)
    {
        bytes += stats.bytes;
        inodes += stats.inodes;

        return *this;
    }

    bool isEmpty() const
    {
        return bytes <= 0 || inodes <= 0;
    }

 };
class IDisk : public Space
{
public:
    /// Default constructor.
    explicit IDisk(std::unique_ptr<Executor> executor_ = std::make_unique<SyncExecutor>()) : executor(std::move(executor_)) { }

    /// calculate unique id (which is presented by uint64_t) for each disk based on the disk type and path.
    virtual UInt64 getID() const = 0;

    /// Root path for all files stored on the disk.
    /// It's not required to be a local filesystem path.
    virtual const String & getPath() const = 0;

    /// Total available space on the disk.
    virtual DiskStats getTotalSpace(bool with_keep_free = false) const = 0;

    /// Space currently available on the disk.
    virtual DiskStats getAvailableSpace() const = 0;

    /// Space available for reservation (available space minus reserved space).
    virtual DiskStats getUnreservedSpace() const = 0;

    /// Amount of bytes which should be kept free on the disk.
    virtual DiskStats getKeepingFreeSpace() const { return {0, 0}; }

    /// Return `true` if the specified file exists.
    virtual bool exists(const String & path) const = 0;

    /// Return `true` if the specified file exists.
    /// If this disk could be DiskByteS3, this function is more recommended than exists()
    virtual bool fileExists(const String & file_path) const { return exists(file_path); }

    /// Return `true` if the specified file exists and it's a regular file (not a directory or special file type).
    virtual bool isFile(const String & path) const = 0;

    /// Return `true` if the specified file exists and it's a directory.
    virtual bool isDirectory(const String & path) const = 0;

    /// Return size of the specified file.
    virtual size_t getFileSize(const String & path) const = 0;

    /// Create directory.
    virtual void createDirectory(const String & path) = 0;

    /// Create directory and all parent directories if necessary.
    virtual void createDirectories(const String & path) = 0;

    /// Remove all files from the directory. Directories are not removed.
    virtual void clearDirectory(const String & path) = 0;

    /// Move directory from `from_path` to `to_path`.
    virtual void moveDirectory(const String & from_path, const String & to_path) = 0;

    /// Return iterator to the contents of the specified directory.
    virtual DiskDirectoryIteratorPtr iterateDirectory(const String & path) = 0;

    /// Return `true` if the specified directory is empty.
    bool isDirectoryEmpty(const String & path);

    /// Create empty file at `path`.
    virtual void createFile(const String & path) = 0;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, an exception will be thrown .
    virtual void moveFile(const String & from_path, const String & to_path) = 0;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, it will be replaced.
    virtual void replaceFile(const String & from_path, const String & to_path) = 0;

    /// Recursively copy data containing at `from_path` to `to_path` located at `to_disk`.
    virtual void copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path);

    /// Copy specified files to `to_disk`, the parent directory of all files should exists.
    virtual void copyFiles(const std::vector<std::pair<String, String>> & files_to_copy, const std::shared_ptr<IDisk> & to_disk);

    /// List files at `path` and add their names to `file_names`
    virtual void listFiles(const String & path, std::vector<String> & file_names) = 0;

    /// Open the file for read and return ReadBufferFromFileBase object.
    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings& settings = {}) const = 0;

    /// Open the file for write and return WriteBufferFromFileBase object.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        const WriteSettings& settings = {}) = 0;

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    virtual void removeFile(const String & path) = 0;

    /// Remove file if it exists.
    virtual void removeFileIfExists(const String & path) = 0;

    /// Remove directory. Throws exception if it's not a directory or if directory is not empty.
    virtual void removeDirectory(const String & path) = 0;

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    virtual void removeRecursive(const String & path) = 0;

    /// Remove file. Throws exception if file doesn't exists or if directory is not empty.
    /// Differs from removeFile for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedFile(const String & path, bool) { removeFile(path); }

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    /// Differs from removeRecursive for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedRecursive(const String & path, bool) { removeRecursive(path); }

    /// Remove file or directory if it exists.
    /// Differs from removeFileIfExists for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedFileIfExists(const String & path, bool) { removeFileIfExists(path); }

    /// Remove CNCH MergeTreeDataPart, only used in ByteS3/ByteHDFS disks
    virtual void removePart(const String &) { throw Exception("removePart is not implemented", ErrorCodes::NOT_IMPLEMENTED); }

    /// Set last modified time to file or directory at `path`.
    virtual void setLastModified(const String & path, const Poco::Timestamp & timestamp) = 0;

    /// Get last modified time of file or directory at `path`.
    virtual Poco::Timestamp getLastModified(const String & path) = 0;

    /// Set file at `path` as read-only.
    virtual void setReadOnly(const String & path) = 0;

    /// Create hardlink from `src_path` to `dst_path`.
    virtual void createHardLink(const String & src_path, const String & dst_path) = 0;

    /// Truncate file to specified size.
    virtual void truncateFile(const String & path, size_t size);

    /// Return disk type - "local", "s3", etc.
    virtual DiskType::Type getType() const = 0;

    /// Invoked when Global Context is shutdown.
    virtual void shutdown() {}

    /// Performs action on disk startup.
    virtual void startup() {}

    /// Return some uniq string for file, overrode for S3
    /// Required for distinguish different copies of the same part on S3
    virtual String getUniqueId(const String & path) const { return path; }

    /// Check file exists and ClickHouse has an access to it
    /// Overrode in DiskS3
    /// Required for S3 to ensure that replica has access to data written by other node
    virtual bool checkUniqueId(const String & id) const { return exists(id); }

    /// Invoked on partitions freeze query.
    virtual void onFreeze(const String &) { }

    /// Returns guard, that insures synchronization of directory metadata with storage device.
    virtual SyncGuardPtr getDirectorySyncGuard(const String & path) const;

    /// Applies new settings for disk in runtime.
    virtual void applyNewSettings(const Poco::Util::AbstractConfiguration &, ContextPtr) {}

    // return ‘true’ if disk support renameTo.
    virtual bool supportRenameTo() { return true; }

    // Get table relative_data_path from disk
    virtual String getTableRelativePathOnDisk(const String & uuid){ return uuid;}

    // A checker to prevent the cluster from writing when enable_cnch_write_remote_disk is disabled
    void assertNotReadonly();

    // Set whether the disk is writable
    void setDiskWritable();

protected:
    friend class DiskDecorator;

    /// Returns executor to perform asynchronous operations.
    virtual Executor & getExecutor() { return *executor; }

private:
    std::unique_ptr<Executor> executor;
    bool is_disk_writable = true;
};

using DiskPtr = std::shared_ptr<IDisk>;
using Disks = std::vector<DiskPtr>;

/**
 * Iterator of directory contents on particular disk.
 */
class IDiskDirectoryIterator
{
public:
    /// Iterate to the next file.
    virtual void next() = 0;

    /// Return `true` if the iterator points to a valid element.
    virtual bool isValid() const = 0;

    /// Path to the file that the iterator currently points to.
    virtual String path() const = 0;

    /// Name of the file that the iterator currently points to.
    virtual String name() const = 0;

    virtual size_t size() const = 0;

    virtual ~IDiskDirectoryIterator() = default;
};

/**
 * Information about reserved size on particular disk.
 */
class IReservation : boost::noncopyable
{
public:
    /// Get reservation size.
    virtual UInt64 getSize() const = 0;

    /// Get i-th disk where reservation take place.
    virtual DiskPtr getDisk(size_t i = 0) const = 0;

    /// Get all disks, used in reservation
    virtual Disks getDisks() const = 0;

    /// Changes amount of reserved space.
    virtual void update(UInt64 new_size) = 0;

    /// Unreserves reserved space.
    virtual ~IReservation() = default;
};

/// Return full path to a file on disk.
inline String fullPath(const DiskPtr & disk, const String & path)
{
    return fs::path(disk->getPath()) / path;
}

/// Return parent path for the specified path.
inline String parentPath(const String & path)
{
    if (path.ends_with('/'))
        return fs::path(path).parent_path().parent_path() / "";
    return fs::path(path).parent_path() / "";
}

/// Return file name for the specified path.
inline String fileName(const String & path)
{
    return fs::path(path).filename();
}

/// Return directory path for the specified path.
inline String directoryPath(const String & path)
{
    return fs::path(path).parent_path() / "";
}

}
