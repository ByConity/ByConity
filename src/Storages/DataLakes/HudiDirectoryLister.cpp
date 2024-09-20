#include "Storages/DataLakes/HudiDirectoryLister.h"
#if USE_HIVE

#include <Storages/Hive/HivePartition.h>

namespace DB
{
HudiCowDirectoryLister::HudiCowDirectoryLister(const DiskPtr & disk_)
    : DiskDirectoryLister(disk_, ILakeScanInfo::StorageType::Hudi, FileScanInfo::FormatType::PARQUET)
{
}

static constexpr auto data_format_suffix = ".parquet";

LakeScanInfos HudiCowDirectoryLister::list(const HivePartitionPtr & partition)
{
    LakeScanInfos lake_scan_infos;
    using FileID = std::string;
    struct FileInfo
    {
        String file_path;
        UInt64 timestamp = 0;
        UInt64 file_size;
    };
    std::unordered_map<FileID, FileInfo> data_files;

    String partition_path = HiveUtil::getPathForListing(partition->location);
    auto it = disk->iterateDirectory(partition_path);
    for (; it->isValid(); it->next())
    {
        if (it->size() == 0 || !endsWith(it->path(), data_format_suffix))
            continue;

        auto key_file = std::filesystem::path(it->path());
        Strings file_parts;
        const String stem = key_file.stem();
        splitInto<'_'>(file_parts, stem);
        if (file_parts.size() != 3)
            continue;

        const auto & file_id = file_parts[0];
        const auto timestamp = parse<UInt64>(file_parts[2]);

        auto & file_info = data_files[file_id];
        if (file_info.timestamp == 0 || file_info.timestamp < timestamp)
        {
            file_info.file_path = it->path();
            file_info.timestamp = timestamp;
            file_info.file_size = it->size();
        }
    }

    for (auto & [_, file_info] : data_files)
    {
        Poco::URI uri(partition->location);
        uri.setPath(file_info.file_path);
        lake_scan_infos.push_back(
            FileScanInfo::create(storage_type, format_type, uri.toString(), file_info.file_size, partition->partition_id));
    }
    return lake_scan_infos;
}

}

#endif
