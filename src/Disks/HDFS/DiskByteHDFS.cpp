/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <filesystem>
#include <memory>
#include <Disks/DiskFactory.h>
#include <Disks/DiskType.h>
#include <Disks/HDFS/DiskByteHDFS.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <IO/Scheduler/IOScheduler.h>
#include <IO/PFRAWSReadBufferFromFS.h>
#include <IO/WSReadBufferFromFS.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include "IO/HDFSRemoteFSReader.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DISK_INDEX;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

DiskPtr DiskByteHDFSReservation::getDisk(size_t i) const
{
    if (i != 0)
    {
        throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
    }
    return disk;
}

class DiskByteHDFSDirectoryIterator : public IDiskDirectoryIterator
{
public:
    DiskByteHDFSDirectoryIterator(HDFSFileSystem & fs, const String & disk_path, const String & dir_path) : hdfs_fs(fs), idx(0)
    {
        base_path = std::filesystem::path(disk_path) / dir_path;

        hdfs_fs.list(base_path, file_names, file_sizes);
    }

    virtual void next() override { ++idx; }

    virtual bool isValid() const override { return idx < file_names.size(); }

    virtual String path() const override { return base_path / name(); }

    virtual String name() const override
    {
        if (idx >= file_names.size())
        {
            throw Exception("Trying to get file name while iterator reach eof", ErrorCodes::BAD_ARGUMENTS);
        }
        return file_names[idx];
    }

    size_t size() const override { return file_sizes.at(idx); }

private:
    HDFSFileSystem & hdfs_fs;

    std::filesystem::path base_path;
    size_t idx;
    std::vector<String> file_names;
    std::vector<size_t> file_sizes;
};

/// TODO: use HDFSCommon replace HDFSFileSystem
DiskByteHDFS::DiskByteHDFS(const String & disk_name_, const String & hdfs_base_path_, const HDFSConnectionParams & hdfs_params_)
    : disk_name(disk_name_), disk_path(hdfs_base_path_), hdfs_params(hdfs_params_), hdfs_fs(hdfs_params_, 10000, 100, 0)
{
    pread_reader_opts = std::make_shared<HDFSRemoteFSReaderOpts>(hdfs_params, true);
    read_reader_opts = std::make_shared<HDFSRemoteFSReaderOpts>(hdfs_params, false);
}

const String & DiskByteHDFS::getName() const
{
    return disk_name;
}

ReservationPtr DiskByteHDFS::reserve(UInt64 bytes)
{
    return std::make_unique<DiskByteHDFSReservation>(static_pointer_cast<DiskByteHDFS>(shared_from_this()), bytes);
}

UInt64 DiskByteHDFS::getID() const
{
    return static_cast<UInt64>(std::hash<String>{}(DiskType::toString(getType())) ^ std::hash<String>{}(getPath()));
}

bool DiskByteHDFS::exists(const String & path) const
{
    return hdfs_fs.exists(absolutePath(path));
}

bool DiskByteHDFS::isFile(const String & path) const
{
    return hdfs_fs.isFile(absolutePath(path));
}

bool DiskByteHDFS::isDirectory(const String & path) const
{
    return hdfs_fs.isDirectory(absolutePath(path));
}

size_t DiskByteHDFS::getFileSize(const String & path) const
{
    return hdfs_fs.getFileSize(absolutePath(path));
}

void DiskByteHDFS::createDirectory(const String & path)
{
    hdfs_fs.createDirectory(absolutePath(path));
}

void DiskByteHDFS::createDirectories(const String & path)
{
    hdfs_fs.createDirectories(absolutePath(path));
}

void DiskByteHDFS::clearDirectory(const String & path)
{
    std::vector<String> file_names;
    std::vector<size_t> file_sizes;
    hdfs_fs.list(absolutePath(path), file_names, file_sizes);
    for (const String & file_name : file_names)
    {
        hdfs_fs.remove(fs::path(disk_path) / path / file_name);
    }
}

void DiskByteHDFS::moveDirectory(const String & from_path, const String & to_path)
{
    hdfs_fs.renameTo(absolutePath(from_path), absolutePath(to_path));
}

DiskDirectoryIteratorPtr DiskByteHDFS::iterateDirectory(const String & path)
{
    return std::make_unique<DiskByteHDFSDirectoryIterator>(hdfs_fs, disk_path, path);
}

void DiskByteHDFS::createFile(const String & path)
{
    hdfs_fs.createFile(absolutePath(path));
}

void DiskByteHDFS::moveFile(const String & from_path, const String & to_path)
{
    hdfs_fs.renameTo(absolutePath(from_path), absolutePath(to_path));
}

void DiskByteHDFS::replaceFile(const String & from_path, const String & to_path)
{
    String from_abs_path = absolutePath(from_path);
    String to_abs_path = absolutePath(to_path);
    if (hdfs_fs.exists(to_abs_path))
    {
        String origin_backup_file = to_abs_path + ".old";
        hdfs_fs.renameTo(to_abs_path, origin_backup_file);
    }
    hdfs_fs.renameTo(from_abs_path, to_abs_path);
}

void DiskByteHDFS::listFiles(const String & path, std::vector<String> & file_names)
{
    std::vector<size_t> file_sizes;
    hdfs_fs.list(absolutePath(path), file_names, file_sizes);
}

std::unique_ptr<ReadBufferFromFileBase> DiskByteHDFS::readFile(const String & path, const ReadSettings & settings) const
{
    String file_path = absolutePath(path);

    if (IO::Scheduler::IOSchedulerSet::instance().enabled() && settings.enable_io_scheduler) {
        if (settings.enable_io_pfra) {
            return std::make_unique<PFRAWSReadBufferFromFS>(
                file_path,
                settings.byte_hdfs_pread ? pread_reader_opts : read_reader_opts,
                IO::Scheduler::IOSchedulerSet::instance().schedulerForPath(file_path),
                PFRAWSReadBufferFromFS::Options{
                    .min_buffer_size_ = settings.remote_fs_buffer_size,
                    .throttler_ = settings.throttler,
                });
        } else {
            return std::make_unique<WSReadBufferFromFS>(
                file_path,
                settings.byte_hdfs_pread ? pread_reader_opts : read_reader_opts,
                IO::Scheduler::IOSchedulerSet::instance().schedulerForPath(file_path),
                settings.remote_fs_buffer_size,
                nullptr,
                0,
                settings.throttler);
        }
    }
    else
    {
        if (settings.remote_fs_prefetch)
        {
            auto impl = std::make_unique<ReadBufferFromByteHDFS>(file_path, hdfs_params,
                settings.byte_hdfs_pread, settings.remote_fs_buffer_size, nullptr, 0, nullptr,
                /* use_external_buffer */true);

            auto global_context = Context::getGlobalContextInstance();
            auto reader = global_context->getThreadPoolReader();
            return std::make_unique<AsynchronousBoundedReadBuffer>(std::move(impl), *reader, settings);
        }
        else
        {
            return std::make_unique<ReadBufferFromByteHDFS>(file_path, hdfs_params,
                settings.byte_hdfs_pread, settings.remote_fs_buffer_size);
        }
    }
}

std::unique_ptr<WriteBufferFromFileBase> DiskByteHDFS::writeFile(const String & path, const WriteSettings & settings)
{
    int write_mode = settings.mode == WriteMode::Append ? (O_APPEND | O_WRONLY) : O_WRONLY;
    return std::make_unique<WriteBufferFromHDFS>(absolutePath(path), hdfs_params, settings.buffer_size, write_mode);
}

void DiskByteHDFS::removeFile(const String & path)
{
    hdfs_fs.remove(absolutePath(path), false);
}

void DiskByteHDFS::removeFileIfExists(const String & path)
{
    String abs_path = absolutePath(path);
    if (hdfs_fs.exists(abs_path))
    {
        hdfs_fs.remove(abs_path, false);
    }
}

void DiskByteHDFS::removeDirectory(const String & path)
{
    hdfs_fs.remove(absolutePath(path), false);
}

void DiskByteHDFS::removeRecursive(const String & path)
{
    hdfs_fs.remove(absolutePath(path), true);
}

void DiskByteHDFS::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    hdfs_fs.setLastModifiedInSeconds(absolutePath(path), timestamp.epochTime());
}

Poco::Timestamp DiskByteHDFS::getLastModified(const String & path)
{
    auto seconds = hdfs_fs.getLastModifiedInSeconds(absolutePath(path));
    return Poco::Timestamp(seconds * 1000 * 1000);
}

void DiskByteHDFS::setReadOnly(const String & path)
{
    hdfs_fs.setWriteable(absolutePath(path), false);
}

void DiskByteHDFS::createHardLink(const String &, const String &)
{
    throw Exception("createHardLink is not supported by DiskByteHDFS", ErrorCodes::NOT_IMPLEMENTED);
}

DiskType::Type DiskByteHDFS::getType() const
{
    return DiskType::Type::ByteHDFS;
}

inline String DiskByteHDFS::absolutePath(const String & relative_path) const
{
    return fs::path(disk_path) / relative_path;
}

void registerDiskByteHDFS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context_) -> DiskPtr {
        String path = config.getString(config_prefix + ".path");
        if (path.empty())
            throw Exception("Disk path can not be empty. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        if (!path.ends_with("/"))
        {
            path.push_back('/');
        }

        String hdfs_params_config_prefix = config_prefix + ".hdfs_params";
        HDFSConnectionParams hdfs_params = config.has(hdfs_params_config_prefix)
            ? HDFSConnectionParams::parseHdfsFromConfig(config, hdfs_params_config_prefix)
            : context_->getHdfsConnectionParams();

        return std::make_shared<DiskByteHDFS>(name, path, hdfs_params);
    };

    // Consider both hdfs & bytehdfs to internal hdfs
    factory.registerDiskType("bytehdfs", creator);
    factory.registerDiskType("hdfs", creator);
}

}
