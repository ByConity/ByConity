
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/DiskCache_fwd.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/RemoteDiskCacheService.h>
#include "Common/Exception.h"
#include "Common/StringUtils/StringUtils.h"
#include "common/logger_useful.h"
#include "common/types.h"
#include "Disks/IDisk.h"
#include "Functions/IFunction.h"
#include <IO/LimitReadBuffer.h>

namespace DB
{

String RemoteDiskCacheService::getFileFullPath(const String & key)
{
    IDiskCachePtr disk_cache;
    if (endsWith(key, DATA_FILE_EXTENSION))
        disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getDataCache();
    disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getMetaCache();

    auto [disk, path] = disk_cache->get(key);
    if (!disk)
    {
        LOG_WARNING(getLogger("RemoteDiskCacheService"), "Can't find the cache key: {}", key);
        return "";
    }

    if (!disk->exists(path))
    {
        LOG_WARNING(
            getLogger("RemoteDiskCacheService"),
            "Find the cache key but the cache data path is not exist: {}->{}",
            key,
            fullPath(disk, path));
        return "";
    }

    return fullPath(disk, path);
}


void RemoteDiskCacheService::writeRemoteFile(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::writeFileRquest * request,
    [[maybe_unused]] ::DB::Protos::writeFileResponse * response,
    ::google::protobuf::Closure * closeure)
{
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    /// this done_guard guarantee to call done->Run() in any situation
    brpc::ClosureGuard done_guard(closeure);

    try
    {
        ThreadPool & pool = IDiskCache::getThreadPool();
        String disk_name = request->disk_name();
        if (disks.find(disk_name) == disks.end())
            throw Exception(fmt::format("Can't find disk name {} when writing file", disk_name), ErrorCodes::LOGICAL_ERROR);

        DiskPtr disk = disks.at(disk_name);

        IDiskCachePtr disk_cache;
        for (const auto & file : request->files())
        {
            if (endsWith(file.key(), DATA_FILE_EXTENSION))
                disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getDataCache();
            disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getMetaCache();

            if (disk_cache->get(file.key()).first)
            {
                LOG_TRACE(log, "Cache file has existed: {}", file.key());
                continue;
            }

            pool.scheduleOrThrowOnError([&, disk_cache, disk, file] {
                try
                {
                    auto data_file = disk->readFile(file.path()); // todo(jiashuo): use default settings
                    data_file->seek(file.offset());
                    LimitReadBuffer segment_value(*data_file, file.length(), false);
                    disk_cache->set(file.key(), segment_value, file.length(), false);
                    LOG_TRACE(log, "Cached remote {} file: {}", disk_cache->getName(), file.key());
                }
                catch (Exception & e)
                {
                    LOG_ERROR(
                        log,
                        "Failed write peer file({}:{}-{}:{}) data: {}",
                        file.key(),
                        file.path(),
                        file.offset(),
                        file.length(),
                        e.message());
                }
            });
        }
    }

    catch (Exception & e)
    {
        LOG_ERROR(log, "Failed write peer file data: {}", e.message());
        cntl->SetFailed(e.message());
    }
}

}
