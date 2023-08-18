#include "Storages/Hive/DirectoryLister.h"
#include <Interpreters/Context_fwd.h>
#include <Poco/Util/MapConfiguration.h>
#if USE_HIVE

#include "Disks/HDFS/DiskByteHDFS.h"
#include "Disks/S3/DiskS3.h"
#include "IO/S3Common.h"
#include "Poco/URI.h"
#include "Storages/Hive/HiveFile/IHiveFile.h"
#include "Storages/Hive/HivePartition.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
}

DiskDirectoryLister::DiskDirectoryLister(const HivePartitionPtr & partition_, const DiskPtr & disk_)
    : partition(partition_), disk(disk_)
{
}

HiveFiles DiskDirectoryLister::list()
{
    HiveFiles hive_files;
    IHiveFile::FileFormat format = IHiveFile::fromHdfsInputFormatClass(partition->file_format);
    Poco::URI uri(partition->location);
    auto it = disk->iterateDirectory(uri.getPath());
    for (; it->isValid(); it->next())
    {
        /// TODO:
        size_t file_size = disk->getFileSize(it->path());
        if (file_size == 0)
            continue;
        HiveFilePtr file = IHiveFile::create(format, it->path(), file_size, disk, partition);
        hive_files.push_back(std::move(file));
    }

    return hive_files;
}

DiskPtr getDiskFromURI(const String & sd_url, const ContextPtr & context)
{
    Poco::URI uri(sd_url);
    const auto & scheme = uri.getScheme();
    if (scheme == "hdfs")
    {
        /// internal
        std::optional<HDFSConnectionParams> params = hdfsParamsFromUrl(uri);
        if (!params)
            params.emplace(context->getHdfsConnectionParams());

        return std::make_shared<DiskByteHDFS>("hive_hdfs", "/", *params);
    }
#if USE_AWS_S3
    else if (scheme == "s3a")
    {
        auto * log = &Poco::Logger::get(__func__);
        LOG_DEBUG(log, "get sd_url: {}", sd_url);
        uri.setScheme("s3"); /// to correctly parse uri
        S3::URI s3_uri(uri);
        // LOG_DEBUG(log, "s3 url: {}", s3_uri.toString());

        /// a bit hack
        Poco::AutoPtr<Poco::Util::MapConfiguration> configuration = new Poco::Util::MapConfiguration;
        String config_prefix = "hive_s3";
        configuration->setString(config_prefix + ".type", "communitys3");
        configuration->setString(config_prefix + ".endpoint", uri.toString());
        configuration->setBool(config_prefix + ".skip_access_check", true);
        configuration->setBool(config_prefix + ".cache_enabled", false);
        return DiskFactory::instance().create("hive_s3_disk", *configuration, config_prefix, context);
    }
#endif

    throw Exception(ErrorCodes::UNKNOWN_STORAGE, "Unknown scheme {}", scheme);
}

}

#endif
