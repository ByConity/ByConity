
#include "Storages/Hive/HiveFile/IHiveFile.h"
#include <common/logger_useful.h>
#if USE_HIVE

#    include <Formats/FormatFactory.h>
#    include "Disks/DiskFactory.h"
#    include "IO/ReadHelpers.h"
#    include "IO/WriteHelpers.h"
#    include "Processors/Formats/IInputFormat.h"
#    include "Protos/hive_models.pb.h"
#    include "Storages/DiskCache/DiskCacheFactory.h"
#    include "Storages/DiskCache/FileDiskCacheSegment.h"
#    include "Storages/DiskCache/IDiskCache.h"
#    include "Storages/Hive/DirectoryLister.h"
#    include "Storages/Hive/HiveFile/HiveORCFile.h"
#    include "Storages/Hive/HiveFile/HiveParquetFile.h"
#    include "Storages/Hive/HivePartition.h"
#    include "Storages/StorageInMemoryMetadata.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int DISK_CACHE_NOT_USED;
    extern const int UNKNOWN_FORMAT;
}

IHiveFile::FileFormat IHiveFile::fromHdfsInputFormatClass(const String & class_name)
{
    const static std::map<String, FileFormat> format_map = {
        {"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", FileFormat::PARQUET},
        {"org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", FileFormat::ORC},
    };

    if (auto it = format_map.find(class_name); it != format_map.end())
        return it->second;
    else
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown hive file format {}", class_name);
}

IHiveFile::FileFormat IHiveFile::fromFormatName(const String & format_name)
{
    const static std::map<String, FileFormat> format_map = {
        {"Parquet", FileFormat::PARQUET},
        {"ORC", FileFormat::ORC},
    };

    if (auto it = format_map.find(format_name); it != format_map.end())
        return it->second;
    else
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown hive file format {}", format_name);
}

String IHiveFile::toString(IHiveFile::FileFormat format)
{
    constexpr static std::array format_names = {"Parquet", "ORC"};
    return format_names[static_cast<int>(format)];
}

HiveFilePtr
IHiveFile::create(FileFormat format, String file_path, size_t file_size, const DiskPtr & disk, const HivePartitionPtr & partition)
{
    HiveFilePtr file;
    if (format == IHiveFile::FileFormat::PARQUET)
    {
        file = std::make_shared<HiveParquetFile>();
    }
    else if (format == IHiveFile::FileFormat::ORC)
    {
        file = std::make_shared<HiveORCFile>();
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown hive file format {}", format);

    file->format = format;
    file->file_path = file_path;
    file->file_size = file_size;
    file->disk = disk;
    file->partition = partition;

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

String IHiveFile::getFormatName() const
{
    return toString(format);
}

std::unique_ptr<ReadBufferFromFileBase> IHiveFile::readFile(const ReadSettings & settings) const
{
    auto * log = &Poco::Logger::get(__func__);
    if (settings.disk_cache_mode < DiskCacheMode::SKIP_DISK_CACHE)
    {
        /// use local cache
        try
        {
            auto cache = DiskCacheFactory::instance().get(DiskCacheType::Hive);
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

        if (settings.disk_cache_mode == DiskCacheMode::FORCE_CHECKSUMS_DISK_CACHE)
        {
            throw Exception(ErrorCodes::DISK_CACHE_NOT_USED, "Hive file {}/{} has no disk cache", disk->getPath(), file_path);
        }
    }
    LOG_TRACE(log, "Read from remote {}/{}, disk_cache_mode {}", disk->getPath(), file_path, settings.disk_cache_mode);
    return disk->readFile(file_path, settings);
}

String IHiveFile::describeMinMaxIndex(const NamesAndTypesList & index_names_and_types) const
{
    WriteBufferFromOwnString buf;
    size_t i = 0;
    for (const auto & name_type : index_names_and_types)
    {
        writeString(name_type.name, buf);
        writeChar(':', buf);
        writeString(name_type.type->getName(), buf);
        writeChar('\n', buf);
        for (const auto & split_minmax : split_minmax_idxes)
        {
            writeString(split_minmax->hyperrectangle[i].toString(), buf);
            writeChar('\n', buf);
        }

        ++i;
    }
    return buf.str();
}

SourcePtr IHiveFile::getReader(const Block & block, const std::shared_ptr<ReadParams> & params)
{
    auto buffer = readFile(params->read_settings);
    auto input_format = FormatFactory::instance().getInput(getFormatName(), *buffer, block, params->context, params->max_block_size);
    input_format->addBuffer(std::move(buffer));
    return input_format;
}

namespace RPCHelpers
{
    void serialize(Protos::ProtoHiveFiles & proto, const HiveFiles & hive_files)
    {
        /// TODO: hack here
        if (!hive_files.empty() && hive_files.front()->partition)
        {
            proto.set_sd_url(hive_files.front()->partition->location);
        }

        for (const auto & hive_file : hive_files)
        {
            auto * proto_file = proto.add_files();
            hive_file->serialize(*proto_file);
        }

        std::cout << proto.DebugString() << std::endl;
    }

    LOG_TRACE(&Poco::Logger::get(__func__), "Proto files {}", proto.DebugString());
}


HiveFiles deserialize(
    const Protos::ProtoHiveFiles & proto,
    const ContextPtr & context,
    const StorageMetadataPtr & metadata,
    const CnchHiveSettings & settings)
{
    HiveFiles files;
    DiskPtr disk;
    std::unordered_map<String, HivePartitionPtr> partition_map;

    for (const auto & file : proto.files())
    {
        if (!disk)
            disk = HiveUtil::getDiskFromURI(proto.sd_url(), context, settings);

        HivePartitionPtr partition;
        if (auto it = partition_map.find(file.partition_id()); it != partition_map.end())
            partition = it->second;
        else if (metadata->hasPartitionKey())
        {
            partition = std::make_shared<HivePartition>();
            partition->load(file.partition_id(), metadata->getPartitionKey());
            partition_map.emplace(file.partition_id(), partition);
        }

        files.emplace_back(
            IHiveFile::create(static_cast<IHiveFile::FileFormat>(file.format()), file.file_path(), file.file_size(), disk, partition));
    }
    return files;
}
}

#endif
