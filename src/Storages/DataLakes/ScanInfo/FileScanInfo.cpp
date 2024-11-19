#include "FileScanInfo.h"

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IInputFormat.h>
#include <Protos/lake_models.pb.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/FileDiskCacheSegment.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/Hive/DirectoryLister.h>
#include <Common/ConsistentHashUtils/Hash.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int DISK_CACHE_NOT_USED;
    extern const int UNKNOWN_FORMAT;
}

FileScanInfo::FormatType FileScanInfo::parseFormatTypeFromString(const String & format_name)
{
    if (format_name == "Parquet")
        return PARQUET;
    if (format_name == "ORC")
        return ORC;
    throw Exception("Unknown format: " + format_name, ErrorCodes::UNKNOWN_FORMAT);
}

static std::string to_string(FileScanInfo::FormatType file_format)
{
    switch (file_format)
    {
        case FileScanInfo::ORC:
            return "ORC";
        case FileScanInfo::PARQUET:
            return "Parquet";
    }
}

FileScanInfo::FileScanInfo(
    StorageType storage_type_, FormatType format_type_, const String & uri_, size_t size_, std::optional<String> partition_id_)
    : FileScanInfo(storage_type_, format_type_, uri_, size_, nullptr, partition_id_)
{
}

FileScanInfo::FileScanInfo(
    const StorageType storage_type_,
    const FormatType format_type_,
    const String & uri_,
    const size_t size_,
    DiskPtr disk_,
    std::optional<String> partition_id_)
    : ILakeScanInfo(storage_type_), format_type(format_type_), uri(uri_), size(size_), disk(disk_)
{
    partition_id = partition_id_;
}

SourcePtr FileScanInfo::getReader(const Block & block, const std::shared_ptr<ReadParams> & read_params)
{
    auto settings = read_params->read_settings;
    settings.remote_fs_prefetch = false;
    settings.local_fs_prefetch = false;
    auto buffer = readFile(settings);
    auto input_format = FormatFactory::instance().getInputFormat(
        to_string(format_type),
        *buffer,
        block,
        read_params->context,
        read_params->max_block_size,
        read_params->format_settings,
        read_params->shared_pool->getThreadsPerStream(),
        std::nullopt,
        true,
        read_params->shared_pool);

    input_format->addBuffer(std::move(buffer));
    if (read_params->query_info)
        input_format->setQueryInfo(*read_params->query_info, read_params->context);
    return input_format;
}

void FileScanInfo::serialize(Protos::LakeScanInfo & proto) const
{
    ILakeScanInfo::serialize(proto);

    auto * file_scan_info = proto.mutable_file_scan_info();
    file_scan_info->set_format_type(format_type);
    file_scan_info->set_uri(uri.toString());
    file_scan_info->set_size(size);
    if (partition_id.has_value())
        file_scan_info->set_partition_id(partition_id.value());
}

LakeScanInfoPtr FileScanInfo::deserialize(
    const Protos::LakeScanInfo & proto,
    const ContextPtr & context,
    const StorageMetadataPtr & /*metadata*/,
    const CnchHiveSettings & settings)
{
    const auto & file_scan_info = proto.file_scan_info();
    auto format_type = static_cast<FormatType>(file_scan_info.format_type());
    auto uri = file_scan_info.uri();
    auto size = file_scan_info.size();

    DiskPtr disk;
    std::optional<String> partition_id;
    disk = HiveUtil::getDiskFromURI(uri, context, settings);
    if (file_scan_info.has_partition_id())
    {
        partition_id = file_scan_info.partition_id();
    }

    return FileScanInfo::create(static_cast<ILakeScanInfo::StorageType>(proto.storage_type()), format_type, uri, size, disk, partition_id);
}

std::unique_ptr<ReadBufferFromFileBase> FileScanInfo::readFile(const ReadSettings & settings) const
{
    auto cache_strategy = DiskCacheFactory::instance().tryGet(DiskCacheType::Hive);
    if (cache_strategy && settings.disk_cache_mode < DiskCacheMode::SKIP_DISK_CACHE)
    {
        /// use local cache
        try
        {
            auto cache = cache_strategy->getDataCache();
            auto [cache_disk, segment_path] = cache->get(uri.getPath());
            if (cache_disk && cache_disk->exists(segment_path))
            {
                LOG_TRACE(log, "Read from local cache {}/{}", cache_disk->getPath(), segment_path);
                return cache_disk->readFile(segment_path);
            }
            cache->cacheSegmentsToLocalDisk({std::make_shared<FileDiskCacheSegment>(disk, uri.getPath(), settings)});
        }
        catch (...)
        {
            tryLogCurrentException(log, "Could not read from local cache");
        }

        if (settings.disk_cache_mode == DiskCacheMode::FORCE_DISK_CACHE)
        {
            throw Exception(ErrorCodes::DISK_CACHE_NOT_USED, "Hive file {}/{} has no disk cache", disk->getPath(), uri.getPath());
        }
    }
    LOG_TRACE(log, "Read from remote {}/{}, disk_cache_mode {}", disk->getPath(), uri.getPath(), settings.disk_cache_mode);
    return disk->readFile(uri.getPath(), settings);
}
}
