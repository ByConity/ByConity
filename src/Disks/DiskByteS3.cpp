/*
 * Copyright 2023 Bytedance Ltd. and/or its affiliates.
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

#include <filesystem>
#include <Disks/DiskByteS3.h>
#include <Disks/DiskFactory.h>
#include <common/logger_useful.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>
#include <Common/formatReadable.h>
#include "IO/ReadSettings.h"
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <IO/PFRAWSReadBufferFromFS.h>
#include <IO/RAReadBufferFromS3.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadBufferFromNexusFS.h>
#include <IO/ReadSettings.h>
#include <IO/S3Common.h>
#include <IO/S3RemoteFSReader.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/PFRAWSReadBufferFromFS.h>
#include <IO/WSReadBufferFromFS.h>
#include <IO/WriteBufferFromS3.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int INCORRECT_DISK_INDEX;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_RMDIR;
    extern const int PATH_ACCESS_DENIED;
}

std::mutex DiskByteS3::reservation_mutex;

class DiskByteS3DirectoryIterator : public IDiskDirectoryIterator
{
public:
    DiskByteS3DirectoryIterator(S3::S3Util * s3_util_, const String & root_prefix, const String & path)
        : s3_util(s3_util_), prefix(std::filesystem::path(root_prefix) / path)
    {
        LOG_TRACE(log, "list s3 bucket {} prefix {}", s3_util->getBucket(), prefix.string());
        res.has_more = true;
        next();
    }

    void next() override
    {
        idx++;
        if (idx >= res.object_names.size() && res.has_more)
        {
            fetchNextBatch();
            idx = 0;
        }
    }

    bool isValid() const override
    {
        return idx < res.object_names.size();
    }

    String path() const override
    {
        return res.object_names.at(idx);
    }

    String name() const override
    {
        return res.object_names.at(idx);
    }

    size_t size() const override
    {
        return res.object_sizes.at(idx);
    }

private:
    void fetchNextBatch()
    {
        res = s3_util->listObjectsWithPrefix(prefix.string(), res.token);
        LOG_TRACE(log, "get {} objs, finished {}", res.object_names.size(), res.has_more);
    }

    S3::S3Util * s3_util = nullptr; /// shared_ptr should be better
    S3::S3Util::S3ListResult res;
    std::filesystem::path prefix;
    size_t idx {0};

    LoggerPtr log{getLogger("DiskByteS3DirectoryIterator")};
};

class DiskByteS3Reservation : public IReservation
{
public:
    DiskByteS3Reservation(const DiskByteS3Ptr & disk_, UInt64 size_)
        : disk(disk_), size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
        {
            throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
        }
        return disk;
    }

    Disks getDisks() const override
    {
        return {disk};
    }

    void update(UInt64 new_size) override
    {
        size = new_size;
    }

private:
    DiskByteS3Ptr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};

DiskByteS3::DiskByteS3(
    const String & name_,
    const String & root_prefix_,
    const String & bucket_,
    const std::shared_ptr<Aws::S3::S3Client> & client_,
    const UInt64 min_upload_part_size_,
    const UInt64 max_single_part_upload_size_)
    : disk_id(next_disk_id.fetch_add(1))
    , name(name_)
    , root_prefix(root_prefix_)
    , s3_util(client_, bucket_, true)
    , reader_opts(std::make_shared<S3RemoteFSReaderOpts>(client_, bucket_))
    , reserved_bytes(0)
    , reservation_count(0)
    , min_upload_part_size(min_upload_part_size_)
    , max_single_part_upload_size(max_single_part_upload_size_)
{
    setDiskWritable();
}

ReservationPtr DiskByteS3::reserve(UInt64 bytes)
{
    return std::make_unique<DiskByteS3Reservation>(std::static_pointer_cast<DiskByteS3>(shared_from_this()), bytes);
}

UInt64 DiskByteS3::getID() const
{
    return static_cast<UInt64>(std::hash<String>{}(DiskType::toString(getType())) ^ std::hash<String>{}(getPath()));
}

bool DiskByteS3::exists(const String& path) const
{
    // exists may used for object or some prefix, so use list instead of head object
    auto res = s3_util.listObjectsWithPrefix(
        std::filesystem::path(root_prefix) / path, std::nullopt, 1);
    return !res.object_names.empty();
}

bool DiskByteS3::fileExists(const String & file_path) const
{
    return s3_util.exists(std::filesystem::path(root_prefix) / file_path);
}

size_t DiskByteS3::getFileSize(const String& path) const
{
    return s3_util.getObjectSize(std::filesystem::path(root_prefix) / path);
}

DiskDirectoryIteratorPtr DiskByteS3::iterateDirectory(const String & path)
{
    return std::make_unique<DiskByteS3DirectoryIterator>(&s3_util, root_prefix, path);
}

void DiskByteS3::listFiles(const String& path, std::vector<String>& file_names)
{
    String prefix = std::filesystem::path(root_prefix) / path;

    S3::S3Util::S3ListResult res;

    do {
        res = s3_util.listObjectsWithPrefix(
            prefix, res.token);
        file_names.reserve(file_names.size() + res.object_names.size());
        file_names.insert(file_names.end(), res.object_names.begin(), res.object_names.end());
    } while(res.has_more);
}

std::unique_ptr<ReadBufferFromFileBase> DiskByteS3::readFile(const String & path, const ReadSettings & settings) const
{
    if (unlikely(settings.remote_fs_read_failed_injection != 0))
    {
        if (settings.remote_fs_read_failed_injection == -1)
            throw Exception("remote_fs_read_failed_injection is enabled and return error immediately", ErrorCodes::LOGICAL_ERROR);
        else
        {
            LOG_TRACE(log, "remote_fs_read_failed_injection is enabled and will sleep {}ms", settings.remote_fs_read_failed_injection);
            std::this_thread::sleep_for(std::chrono::milliseconds(settings.remote_fs_read_failed_injection));
        }
    }

    String object_key = std::filesystem::path(root_prefix) / path;
    if (IO::Scheduler::IOSchedulerSet::instance().enabled() && settings.enable_io_scheduler) {
        if (settings.enable_io_pfra) {
            return std::make_unique<PFRAWSReadBufferFromFS>(
                object_key,
                reader_opts,
                IO::Scheduler::IOSchedulerSet::instance().schedulerForPath(object_key),
                PFRAWSReadBufferFromFS::Options{
                    .min_buffer_size_ = settings.remote_fs_buffer_size,
                    .throttler_ = settings.remote_throttler,
                });
        } else {
            return std::make_unique<WSReadBufferFromFS>(
                object_key,
                reader_opts,
                IO::Scheduler::IOSchedulerSet::instance().schedulerForPath(object_key),
                settings.remote_fs_buffer_size,
                nullptr,
                0,
                settings.remote_throttler);
        }
    }
    else
    {
        ReadSettings modified_settings{settings};
        modified_settings.for_disk_s3 = true;
        auto nexus_fs = settings.enable_nexus_fs ? Context::getGlobalContextInstance()->getNexusFS() : nullptr;
        bool use_external_buffer = nexus_fs ? false : settings.remote_fs_prefetch;
        std::unique_ptr<ReadBufferFromFileBase> impl;
        impl = std::make_unique<ReadBufferFromS3>(
            s3_util.getClient(), s3_util.getBucket(), object_key, modified_settings, 3, false, use_external_buffer);

        if (nexus_fs)
        {
            impl = std::make_unique<ReadBufferFromNexusFS>(
                settings.local_fs_buffer_size,
                settings.remote_fs_prefetch,
                std::move(impl),
                *nexus_fs);
        }
        else if (settings.remote_fs_prefetch)
        {
            auto impl = std::make_unique<ReadBufferFromS3>(s3_util.getClient(),
                s3_util.getBucket(), object_key, modified_settings, 3, false, /* use_external_buffer */true);

            auto global_context = Context::getGlobalContextInstance();
            auto reader = global_context->getThreadPoolReader();
            return std::make_unique<AsynchronousBoundedReadBuffer>(std::move(impl), *reader, modified_settings);
        }
        else
        {
            return std::make_unique<ReadBufferFromS3>(s3_util.getClient(),
                s3_util.getBucket(), object_key, modified_settings, 3, false);
        }
    }
}

std::unique_ptr<WriteBufferFromFileBase> DiskByteS3::writeFile(const String & path, const WriteSettings & settings)
{
    assertNotReadonly();

    if (unlikely(settings.remote_fs_write_failed_injection != 0))
    {
        if (settings.remote_fs_write_failed_injection == -1)
            throw Exception("remote_fs_write_failed_injection is enabled and return error immediately", ErrorCodes::LOGICAL_ERROR);
        else
        {
            LOG_TRACE(log, "remote_fs_write_failed_injection is enabled and will sleep {}ms", settings.remote_fs_write_failed_injection);
            std::this_thread::sleep_for(std::chrono::milliseconds(settings.remote_fs_write_failed_injection));
        }
    }

    {
        return std::make_unique<WriteBufferFromS3>(
            s3_util.getClient(),
            s3_util.getBucket(),
            std::filesystem::path(root_prefix) / path,
            max_single_part_upload_size,
            min_upload_part_size,
            settings.file_meta,
            settings.buffer_size,
            false,
            false,
            8,
            true);
    }
}

void DiskByteS3::removeFile(const String& path)
{
    assertNotReadonly();
    s3_util.deleteObject(std::filesystem::path(root_prefix) / path);
}

void DiskByteS3::removeFileIfExists(const String& path)
{
    assertNotReadonly();
    s3_util.deleteObject(std::filesystem::path(root_prefix) / path, false);
}

void DiskByteS3::removeDirectory(const String & path)
{
    assertNotReadonly();
    String prefix = std::filesystem::path(root_prefix) / path;
    auto res = s3_util.listObjectsWithPrefix(prefix, std::nullopt, /*limit*/ 1);
    if (!res.object_names.empty())
        throw Exception(ErrorCodes::CANNOT_RMDIR, "Cannot remove directory {} because it's not empty", prefix);
}

void DiskByteS3::removeRecursive(const String& path)
{
    assertNotReadonly();
    String prefix = std::filesystem::path(root_prefix) / path;

    LOG_TRACE(log, "RemoveRecursive: {} - {}", prefix, path);

    s3_util.deleteObjectsWithPrefix(prefix, [](const S3::S3Util&, const String&){return true;});
}

void DiskByteS3::copyFile(const String & from_path, const String & to_bucket, const String & to_path)
{
    String full_from_path = std::filesystem::path(root_prefix) / from_path;
    s3_util.copyObject(full_from_path, to_bucket, to_path);
    LOG_TRACE(log, "Copy file from {} to {}", from_path, to_path);
}

static void checkWriteAccess(IDisk & disk)
{
    auto file = disk.writeFile("test_acl", {.buffer_size = DBMS_DEFAULT_BUFFER_SIZE, .mode = WriteMode::Rewrite});
    file->write("test", 4);
}

static void checkReadAccess(const String & disk_name, IDisk & disk)
{
    auto file = disk.readFile("test_acl", ReadSettings().initializeReadSettings(4));
    String buf(4, '0');
    file->readStrict(buf.data(), 4);
    if (buf != "test")
        throw Exception("No read access to S3 bucket in disk " + disk_name, ErrorCodes::PATH_ACCESS_DENIED);
}

static void checkRemoveAccess(IDisk & disk) { disk.removeFile("test_acl"); }

void registerDiskByteS3(DiskFactory& factory)
{
    auto creator = [](const String& name,
        const Poco::Util::AbstractConfiguration& config, const String& config_prefix,
            ContextPtr) -> DiskPtr {
        S3::S3Config s3_cfg(config, config_prefix);
        std::shared_ptr<Aws::S3::S3Client> client = s3_cfg.create();

        // initialize cfs
        auto s3disk = std::make_shared<DiskByteS3>(name, s3_cfg.root_prefix, s3_cfg.bucket, client,
            s3_cfg.min_upload_part_size, s3_cfg.max_single_part_upload_size);

        if (!config.getBool(config_prefix + ".skip_access_check", true))
        {
            checkWriteAccess(*s3disk);
            checkReadAccess(name, *s3disk);
            checkRemoveAccess(*s3disk);
        }
        return s3disk;
    };
    factory.registerDiskType("bytes3", creator);
    factory.registerDiskType("s3", creator);
}

}
