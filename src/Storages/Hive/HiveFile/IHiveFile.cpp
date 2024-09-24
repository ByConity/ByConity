#include "Storages/Hive/HiveFile/IHiveFile.h"
#include <optional>
#if USE_HIVE

#include "Disks/DiskFactory.h"
#include "Formats/FormatFactory.h"
#include "IO/ReadHelpers.h"
#include "IO/WriteHelpers.h"
#include "Storages/DiskCache/DiskCacheFactory.h"
#include "Storages/DiskCache/FileDiskCacheSegment.h"
#include "Storages/DiskCache/IDiskCache.h"
#include "Storages/DataLakes/HiveFile/HiveHudiFile.h"
#include "Storages/DataLakes/HiveFile/HiveInputSplitFile.h"
#include "Storages/Hive/DirectoryLister.h"
#include "Storages/Hive/HiveFile/HiveORCFile.h"
#include "Storages/Hive/HiveFile/HiveParquetFile.h"
#include "Storages/Hive/HivePartition.h"
#include "Storages/StorageInMemoryMetadata.h"
#include "Processors/Formats/IInputFormat.h"
#include "Protos/hive_models.pb.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int DISK_CACHE_NOT_USED;
    extern const int UNKNOWN_FORMAT;
}

IHiveFile::FileFormat IHiveFile::fromFormatName(const String & format_name)
{
    const static std::map<String, FileFormat> format_map = {
        {"Parquet", FileFormat::PARQUET},
        {"ORC", FileFormat::ORC},
        {"HUDI", FileFormat::HUDI},
        {"InputSplit", FileFormat::InputSplit},
    };

    if (auto it = format_map.find(format_name); it != format_map.end())
        return it->second;
    else
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown hive file format {}", format_name);
}

String IHiveFile::toString(IHiveFile::FileFormat format)
{
    constexpr static std::array format_names = {"Parquet", "ORC", "HUDI", "InputSplit"};
    return format_names[static_cast<int>(format)];
}

HiveFilePtr IHiveFile::create(
    FileFormat format,
    String file_path,
    size_t file_size,
    const DiskPtr & disk,
    const HivePartitionPtr & partition)
{
    HiveFilePtr file;
    /// TODO: change to factory
    if (format == IHiveFile::FileFormat::PARQUET)
    {
        file = std::make_shared<HiveParquetFile>();
    }
    else if (format == IHiveFile::FileFormat::ORC)
    {
        file = std::make_shared<HiveORCFile>();
    }
#if USE_JAVA_EXTENSIONS
    else if (format == IHiveFile::FileFormat::HUDI)
    {
        file = std::make_shared<HiveHudiFile>();
    }
    else if (format == IHiveFile::FileFormat::InputSplit)
    {
        file = std::make_shared<HiveInputSplitFile>();
    }
#endif
    else
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown hive file format {}", format);

    file->load(format, file_path, file_size, disk, partition);
    return file;
}

void IHiveFile::serialize(Protos::ProtoHiveFile & proto) const
{
    proto.set_format(static_cast<int>(format));
    proto.set_file_path(file_path);
    proto.set_file_size(file_size);

    if (partition)
        proto.set_partition_id(partition->partition_id);
}

void IHiveFile::deserialize(const Protos::ProtoHiveFile & proto)
{
    format = static_cast<FileFormat>(proto.format());
    file_path = proto.file_path();
    file_size = proto.file_size();
}

String IHiveFile::getFormatName() const
{
    return toString(format);
}

std::unique_ptr<ReadBufferFromFileBase> IHiveFile::readFile(const ReadSettings & settings) const
{
    auto * log = &Poco::Logger::get(__func__);
    auto cache_strategy = DiskCacheFactory::instance().tryGet(DiskCacheType::Hive);
    if (cache_strategy && settings.disk_cache_mode < DiskCacheMode::SKIP_DISK_CACHE)
    {
        /// use local cache
        try
        {
            auto cache = cache_strategy->getDataCache();
            auto [cache_disk, segment_path] = cache->get(file_path);
            if (cache_disk && cache_disk->exists(segment_path))
            {
                LOG_TRACE(log, "Read from local cache {}/{}", cache_disk->getPath(), segment_path);
                return cache_disk->readFile(segment_path);
            }
            cache->cacheSegmentsToLocalDisk({std::make_shared<FileDiskCacheSegment>(disk, file_path, settings)});
        }
        catch (...)
        {
            tryLogCurrentException(log, "Could not read from local cache");
        }

        if (settings.disk_cache_mode == DiskCacheMode::FORCE_DISK_CACHE)
        {
            throw Exception(ErrorCodes::DISK_CACHE_NOT_USED, "Hive file {}/{} has no disk cache", disk->getPath(), file_path);
        }
    }
    LOG_TRACE(log, "Read from remote {}/{}, disk_cache_mode {}", disk->getPath(), file_path, settings.disk_cache_mode);
    return disk->readFile(file_path, settings);
}

SourcePtr IHiveFile::getReader(const Block & block, const std::shared_ptr<ReadParams> & params)
{
    auto settings = params->read_settings;
    settings.remote_fs_prefetch = false;
    settings.local_fs_prefetch = false;
    auto buffer = readFile(settings);
    auto input_format = FormatFactory::instance().getInputFormat(
        getFormatName(),
        *buffer,
        block,
        params->context,
        params->max_block_size,
        params->format_settings,
        params->shared_pool->getThreadsPerStream(),
        std::nullopt,
        true,
        params->shared_pool);

    input_format->addBuffer(std::move(buffer));
    if (params->query_info)
        input_format->setQueryInfo(*params->query_info, params->context);
    return input_format;
}

void IHiveFile::load(FileFormat format_, const String & file_path_, size_t file_size_, const DiskPtr & disk_, const HivePartitionPtr & partition_)
{
    format = format_;
    file_path = file_path_;
    file_size = file_size_;
    disk = disk_;
    partition = partition_;
}

namespace RPCHelpers
{

HiveFiles deserialize(
    const Protos::ProtoHiveFiles & proto,
    const ContextPtr & context,
    const StorageMetadataPtr & metadata,
    const CnchHiveSettings & settings)
{
    HiveFiles files;
    DiskPtr disk;
    std::unordered_map<String, HivePartitionPtr> partition_map;

    for (const auto & file_proto : proto.files())
    {
        /// TODO: {caoliu} hack here, this can be bad
        if (!disk && !proto.sd_url().empty())
            disk = HiveUtil::getDiskFromURI(proto.sd_url(), context, settings);

        HivePartitionPtr partition;
        if (auto it = partition_map.find(file_proto.partition_id()); it != partition_map.end())
            partition = it->second;
        else if (metadata->hasPartitionKey())
        {
            partition = std::make_shared<HivePartition>();
            partition->load(file_proto.partition_id(), metadata->getPartitionKey());
            partition_map.emplace(file_proto.partition_id(), partition);
        }

        auto hive_file = IHiveFile::create(
            static_cast<IHiveFile::FileFormat>(file_proto.format()),
            file_proto.file_path(),
            file_proto.file_size(),
            disk,
            partition);
        hive_file->deserialize(file_proto);
        files.push_back(std::move(hive_file));
    }
    return files;
}

}

}

#endif
