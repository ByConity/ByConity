#include "Storages/DataLakes/HudiDirectoryLister.h"
#if USE_HIVE

#include "Storages/Hive/HiveFile/IHiveFile.h"
#include "Storages/Hive/HivePartition.h"

namespace DB
{
HudiCowDirectoryLister::HudiCowDirectoryLister(const DiskPtr & disk_)
    : DiskDirectoryLister(disk_, IHiveFile::FileFormat::PARQUET)
{
}

static constexpr auto data_format = IHiveFile::FileFormat::PARQUET;
static constexpr auto data_format_suffix = ".parquet";

HiveFiles HudiCowDirectoryLister::list(const HivePartitionPtr & partition)
{
    HiveFiles hive_files;
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
        HiveFilePtr file = IHiveFile::create(data_format, std::move(file_info.file_path), file_info.file_size, disk, partition);
        hive_files.push_back(std::move(file));
    }
    return hive_files;
}

}

#endif
