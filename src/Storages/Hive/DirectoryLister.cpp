#include "Storages/Hive/DirectoryLister.h"
#include <Interpreters/Context.h>
#include <Poco/Util/MapConfiguration.h>
#if USE_HIVE

#include "Poco/URI.h"
#include "Poco/Util/MapConfiguration.h"
#include "Common/StringUtils/StringUtils.h"
#include "Disks/HDFS/DiskByteHDFS.h"
#include "Disks/S3/DiskS3.h"
#include "IO/S3Common.h"
#include "Interpreters/Context.h"
#include "Storages/DataLakes/HudiDirectoryLister.h"
#include "Storages/Hive/CnchHiveSettings.h"
#include "Storages/Hive/HiveFile/IHiveFile.h"
#include "Storages/Hive/HivePartition.h"
#include "Storages/Hive/StorageCnchHive.h"

#include <hive_metastore_types.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
}

namespace HiveUtil
{
String getPath(const String & path)
{
    String encoded;
    Poco::URI::encode(path, "", encoded);
    Poco::URI uri(encoded);
    const String & scheme = uri.getScheme();
    if (scheme == "hdfs" || scheme == "file")
    {
        return uri.getPath();
    }
    else if (scheme == "s3a" || scheme == "s3")
    {
        uri.setScheme("s3"); /// to correctly parse uri
        S3::URI s3_uri(uri);
        return s3_uri.key;
    }

    throw Exception(ErrorCodes::UNKNOWN_STORAGE, "Unknown scheme {}", scheme);
}

String getPathForListing(const String & path)
{
    String dir_path = getPath(path);
    if (!endsWith(dir_path, "/"))
    {
        dir_path += '/';
    }
    return dir_path;
}

DiskPtr getDiskFromURI(const String & sd_url, const ContextPtr & context, const CnchHiveSettings & settings)
{
    String encoded_sd_url;
    Poco::URI::encode(sd_url, "", encoded_sd_url);
    Poco::URI uri(encoded_sd_url);
    auto * log = &Poco::Logger::get(__func__);
    LOG_TRACE(log, "sd_url: {}\n encoded {}", sd_url, encoded_sd_url);
    const auto & scheme = uri.getScheme();
    if (scheme == "hdfs")
    {
        if (!settings.hdfs_fs.value.empty())
        {
            Poco::URI new_uri(settings.hdfs_fs.value);
            new_uri.setPath(uri.getPath());
            uri = new_uri;
        }
        /// internal
        std::optional<HDFSConnectionParams> params = hdfsParamsFromUrl(uri);
        if (!params)
            params.emplace(context->getHdfsConnectionParams());

        return std::make_shared<DiskByteHDFS>("hive_hdfs", "/", *params);
    }
#if USE_AWS_S3
    else if (scheme == "s3a" || scheme == "s3")
    {
        LOG_DEBUG(log, "sd_url: {}", sd_url);
        uri.setScheme("s3"); /// to correctly parse uri
        if (!endsWith(uri.getPath(), "/"))
            uri.setPath(uri.getPath() + "/");

        S3::URI s3_uri(uri);
        // LOG_DEBUG(log, "s3 url: {}", s3_uri.toString());

        Poco::AutoPtr<Poco::Util::MapConfiguration> configuration = new Poco::Util::MapConfiguration;
        String config_prefix = "hive_s3";
        configuration->setString(config_prefix + ".type", "bytes3");
        configuration->setString(config_prefix + ".endpoint", settings.endpoint);
        configuration->setString(config_prefix + ".region", settings.region);
        configuration->setString(config_prefix + ".bucket", s3_uri.bucket);
        configuration->setString(config_prefix + ".path", "");
        configuration->setString(config_prefix + ".ak_id", settings.ak_id);
        configuration->setString(config_prefix + ".ak_secret", settings.ak_secret);
        configuration->setBool(config_prefix + ".is_virtual_hosted_style", settings.hive_remote_virtual_hosted_style);

#ifndef NDEBUG
        /// check that we can read from the s3 disk
        configuration->setBool(config_prefix + ".skip_access_check", false);
#endif
        return DiskFactory::instance().create("hive_s3_disk", *configuration, config_prefix, context);
    }
#endif

    throw Exception(ErrorCodes::UNKNOWN_STORAGE, "Unknown scheme {} in url {}", scheme, sd_url);
}
}

DiskDirectoryLister::DiskDirectoryLister(const DiskPtr & disk_, IHiveFile::FileFormat format)
    : disk(disk_), file_format(format)
{
}

HiveFiles DiskDirectoryLister::list(const HivePartitionPtr & partition)
{
    HiveFiles hive_files;

    std::vector<String> file_names;
    String partition_path = HiveUtil::getPathForListing(partition->location);
    auto it = disk->iterateDirectory(partition_path);
    for (; it->isValid(); it->next())
    {
        if (it->size() == 0)
            continue;
        HiveFilePtr file = IHiveFile::create(file_format, it->path(), it->size(), disk, partition);
        hive_files.push_back(std::move(file));
    }

    return hive_files;
}


}

#endif
