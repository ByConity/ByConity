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

#include "DiskLocal.h"
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <Common/createHardLink.h>
#include <Disks/DiskFactory.h>
#include <Disks/IDisk.h>

#include <Disks/LocalDirectorySyncGuard.h>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>
#include <IO/createReadBufferFromFileBase.h>
#include <Poco/Logger.h>

#include <filesystem>
#include <fstream>
#include <unistd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int PATH_ACCESS_DENIED;
    extern const int INCORRECT_DISK_INDEX;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_UNLINK;
    extern const int CANNOT_RMDIR;
}

std::mutex DiskLocal::reservation_mutex;


using DiskLocalPtr = std::shared_ptr<DiskLocal>;

class DiskLocalReservation : public IReservation
{
public:
    DiskLocalReservation(const DiskLocalPtr & disk_, UInt64 size_)
        : disk(disk_), size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk(size_t i) const override;

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override;

    ~DiskLocalReservation() override;

private:
    DiskLocalPtr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};


class DiskLocalDirectoryIterator : public IDiskDirectoryIterator
{
public:
    explicit DiskLocalDirectoryIterator(const String & disk_path_, const String & dir_path_)
        : dir_path(dir_path_), entry(fs::path(disk_path_) / dir_path_)
    {
    }

    void next() override { ++entry; }

    bool isValid() const override { return entry != fs::directory_iterator(); }

    String path() const override
    {
        if (entry->is_directory())
            return dir_path / entry->path().filename() / "";
        else
            return dir_path / entry->path().filename();
    }


    String name() const override { return entry->path().filename(); }

    size_t size() const override { return entry->file_size(); }

private:
    fs::path dir_path;
    fs::directory_iterator entry;
};

UInt64 DiskLocal::getID() const
{
    return static_cast<UInt64>(std::hash<String>{}(DiskType::toString(getType())) ^ std::hash<String>{}(getPath()));
}

ReservationPtr DiskLocal::reserve(UInt64 bytes)
{
    if (!tryReserve(bytes))
        return {};
    return std::make_unique<DiskLocalReservation>(std::static_pointer_cast<DiskLocal>(shared_from_this()), bytes);
}

bool DiskLocal::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(DiskLocal::reservation_mutex);
    if (bytes == 0)
    {
        LOG_DEBUG(log, "Reserving 0 bytes on disk {}", backQuote(name));
        ++reservation_count;
        return true;
    }

    auto available_space = getAvailableSpace();
    auto unreserved_space
        = available_space - DiskStats{std::min(available_space.bytes, reserved_bytes), std::min(available_space.inodes, reserved_inodes)};
    if (unreserved_space.bytes >= bytes)
    {
        LOG_TRACE(
            log,
            "Reserving {} on disk {}(free {}({})), having unreserved {}({}).",
            ReadableSize(bytes),
            backQuote(name),
            ReadableSize(available_space.bytes),
            available_space.inodes,
            ReadableSize(unreserved_space.bytes),
            unreserved_space.inodes);
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }

    LOG_WARNING(
        log,
        "Can't reserving {} on disk {}(free {}({})), having unreserved {}({}).",
        ReadableSize(bytes),
        backQuote(name),
        ReadableSize(available_space.bytes),
        available_space.inodes,
        ReadableSize(unreserved_space.bytes),
        unreserved_space.inodes);
    return false;
}

DiskStats DiskLocal::getTotalSpace(bool with_keep_free) const
{
    struct statvfs fs;
    if (name == "default") /// for default disk we get space from path/data/
        fs = getStatVFS((fs::path(disk_path) / "data/").string());
    else
        fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_blocks * fs.f_bsize;
    UInt64 total_inode = fs.f_files;
    if (!with_keep_free)
    {
        if (total_size < keep_free_disk_stats.bytes || total_inode < keep_free_disk_stats.inodes)
            return {0, 0};
        return {total_size - keep_free_disk_stats.bytes, total_inode - keep_free_disk_stats.inodes};
    }

    return {total_size, total_inode};
}

DiskStats DiskLocal::getAvailableSpace() const
{
    /// we use f_bavail, because part of b_free space is
    /// available for superuser only and for system purposes
    struct statvfs fs;
    if (name == "default") /// for default disk we get space from path/data/
        fs = getStatVFS((fs::path(disk_path) / "data/").string());
    else
        fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_bavail * fs.f_bsize;
    UInt64 total_inode = fs.f_favail;
    if (total_size < keep_free_disk_stats.bytes || total_inode < keep_free_disk_stats.inodes)
        return {0, 0};
    return {total_size - keep_free_disk_stats.bytes, total_inode - keep_free_disk_stats.inodes};
}

DiskStats DiskLocal::getUnreservedSpace() const
{
    std::lock_guard lock(DiskLocal::reservation_mutex);
    auto available_space = getAvailableSpace();
    available_space -= {std::min(available_space.bytes, reserved_bytes), std::min(available_space.inodes, reserved_inodes)};
    return available_space;
}

bool DiskLocal::exists(const String & path) const
{
    return fs::exists(fs::path(disk_path) / path);
}

bool DiskLocal::isFile(const String & path) const
{
    return fs::is_regular_file(fs::path(disk_path) / path);
}

bool DiskLocal::isDirectory(const String & path) const
{
    return fs::is_directory(fs::path(disk_path) / path);
}

size_t DiskLocal::getFileSize(const String & path) const
{
    return fs::file_size(fs::path(disk_path) / path);
}

void DiskLocal::createDirectory(const String & path)
{
    fs::create_directory(fs::path(disk_path) / path);
}

void DiskLocal::createDirectories(const String & path)
{
    fs::create_directories(fs::path(disk_path) / path);
}

void DiskLocal::clearDirectory(const String & path)
{
    for (const auto & entry : fs::directory_iterator(fs::path(disk_path) / path))
        fs::remove(entry.path());
}

void DiskLocal::moveDirectory(const String & from_path, const String & to_path)
{
    fs::rename(fs::path(disk_path) / from_path, fs::path(disk_path) / to_path);
}

DiskDirectoryIteratorPtr DiskLocal::iterateDirectory(const String & path)
{
    return std::make_unique<DiskLocalDirectoryIterator>(disk_path, path);
}

void DiskLocal::moveFile(const String & from_path, const String & to_path)
{
    fs::rename(fs::path(disk_path) / from_path, fs::path(disk_path) / to_path);
}

void DiskLocal::replaceFile(const String & from_path, const String & to_path)
{
    fs::path from_file = fs::path(disk_path) / from_path;
    fs::path to_file = fs::path(disk_path) / to_path;
    fs::rename(from_file, to_file);
}

std::unique_ptr<ReadBufferFromFileBase>
DiskLocal::readFile(
    const String & path, const ReadSettings& settings) const
{
    return createReadBufferFromFileBase(fs::path(disk_path) / path, settings);
}

std::unique_ptr<WriteBufferFromFileBase>
DiskLocal::writeFile(const String & path, const WriteSettings& settings)
{
    int flags = (settings.mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    return std::make_unique<WriteBufferFromFile>(fs::path(disk_path) / path, settings.buffer_size, flags);
}

void DiskLocal::removeFile(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != unlink(fs_path.c_str()))
        throwFromErrnoWithPath("Cannot unlink file " + fs_path.string(), fs_path, ErrorCodes::CANNOT_UNLINK);
}

void DiskLocal::removeFileIfExists(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != unlink(fs_path.c_str()) && errno != ENOENT)
        throwFromErrnoWithPath("Cannot unlink file " + fs_path.string(), fs_path, ErrorCodes::CANNOT_UNLINK);
}

void DiskLocal::removeDirectory(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != rmdir(fs_path.c_str()))
        throwFromErrnoWithPath("Cannot rmdir " + fs_path.string(), fs_path, ErrorCodes::CANNOT_RMDIR);
}

void DiskLocal::removeRecursive(const String & path)
{
    fs::remove_all(fs::path(disk_path) / path);
}

void DiskLocal::listFiles(const String & path, std::vector<String> & file_names)
{
    file_names.clear();
    for (const auto & entry : fs::directory_iterator(fs::path(disk_path) / path))
        file_names.emplace_back(entry.path().filename());
}

void DiskLocal::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    FS::setModificationTime(fs::path(disk_path) / path, timestamp.epochTime());
}

Poco::Timestamp DiskLocal::getLastModified(const String & path)
{
    return FS::getModificationTimestamp(fs::path(disk_path) / path);
}

void DiskLocal::createHardLink(const String & src_path, const String & dst_path)
{
    DB::createHardLink(fs::path(disk_path) / src_path, fs::path(disk_path) / dst_path);
}

void DiskLocal::truncateFile(const String & path, size_t size)
{
    int res = truncate((fs::path(disk_path) / path).string().data(), size);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot truncate file " + path, path, ErrorCodes::CANNOT_TRUNCATE_FILE);
}

void DiskLocal::createFile(const String & path)
{
    FS::createFile(fs::path(disk_path) / path);
}

void DiskLocal::setReadOnly(const String & path)
{
    fs::permissions(fs::path(disk_path) / path,
                    fs::perms::owner_write | fs::perms::group_write | fs::perms::others_write,
                    fs::perm_options::remove);
}

bool inline isSameDiskType(const IDisk & one, const IDisk & another)
{
    return typeid(one) == typeid(another);
}

void DiskLocal::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    if (isSameDiskType(*this, *to_disk))
    {
        fs::path to = fs::path(to_disk->getPath()) / to_path;
        fs::path from = fs::path(disk_path) / from_path;
        if (from_path.ends_with('/'))
            from = from.parent_path();
        if (fs::is_directory(from))
            to /= from.filename();

        fs::copy(from, to, fs::copy_options::recursive | fs::copy_options::overwrite_existing); /// Use more optimal way.
    }
    else
        IDisk::copy(from_path, to_disk, to_path); /// Copy files through buffers.
}

SyncGuardPtr DiskLocal::getDirectorySyncGuard(const String & path) const
{
    return std::make_unique<LocalDirectorySyncGuard>(fs::path(disk_path) / path);
}

DiskPtr DiskLocalReservation::getDisk(size_t i) const
{
    if (i != 0)
    {
        throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
    }
    return disk;
}

void DiskLocalReservation::update(UInt64 new_size)
{
    std::lock_guard lock(DiskLocal::reservation_mutex);
    disk->reserved_bytes -= size;
    size = new_size;
    disk->reserved_bytes += size;
}


DiskLocalReservation::~DiskLocalReservation()
{
    try
    {
        std::lock_guard lock(DiskLocal::reservation_mutex);
        if (disk->reserved_bytes < size)
        {
            disk->reserved_bytes = 0;
            LOG_ERROR(disk->log, "Unbalanced reservations size for disk '{}'.", disk->getName());
        }
        else
        {
            disk->reserved_bytes -= size;
        }

        if (disk->reservation_count == 0)
            LOG_ERROR(disk->log, "Unbalanced reservation count for disk '{}'.", disk->getName());
        else
            --disk->reservation_count;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void registerDiskLocal(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      const DisksMap & /* disk_map */) -> DiskPtr {
        String path = config.getString(config_prefix + ".path", "");
        if (name == "default")
        {
            if (!path.empty())
                throw Exception(
                    "\"default\" disk path should be provided in <path> not it <storage_configuration>",
                    ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
            path = context->getPath();
        }

        if (path.empty())
            throw Exception("Disk path can not be empty. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        if (path.back() != '/')
            path.push_back('/');

        bool has_space_ratio = config.has(config_prefix + ".keep_free_space_ratio");

        if (config.has(config_prefix + ".keep_free_space_bytes") && has_space_ratio)
            throw Exception(
                "Only one of 'keep_free_space_bytes' and 'keep_free_space_ratio' can be specified",
                ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        UInt64 keep_free_space_bytes
            = std::max(config.getUInt64(config_prefix + ".keep_free_space_bytes", 0), config.getUInt64("global_keep_free_space_bytes", 0));
        UInt64 keep_free_inode_count = std::max(
            config.getUInt64(config_prefix + ".keep_free_space_inodes", 0), config.getUInt64("global_keep_free_space_inodes", 0));
        double ratio = std::max(
            config.getDouble(config_prefix + ".keep_free_space_ratio", 0),
            config.getDouble("global_keep_free_space_ratio", 0.05));

        if (ratio < 0 || ratio > 1)
            throw Exception("'keep_free_space_ratio' have to be between 0 and 1", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        String tmp_path = path;
        if (tmp_path.empty())
            tmp_path = context->getPath();

        if (!fs::exists(tmp_path))
        {
            LOG_WARNING(getLogger("DiskLocal"), "Can't find path {} so keep-free is forced to set zero", tmp_path);
            return std::make_shared<DiskLocal>(name, path, DiskStats{});
        }

        // Create tmp disk for getting total disk space.
        auto tmp_disk = DiskLocal("tmp", tmp_path, {});
        keep_free_space_bytes = std::max(static_cast<UInt64>(tmp_disk.getTotalSpace().bytes * ratio), keep_free_space_bytes);
        keep_free_inode_count = std::max(static_cast<UInt64>(tmp_disk.getTotalSpace().inodes * ratio), keep_free_inode_count);
        return std::make_shared<DiskLocal>(name, path, DiskStats{keep_free_space_bytes, keep_free_inode_count});
    };
    factory.registerDiskType("local", creator);
}

}
