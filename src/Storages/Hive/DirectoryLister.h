#pragma once

#include "Common/config.h"
#if USE_HIVE

#include "Storages/Hive/HiveFile/IHiveFile.h"
#include "Disks/IDisk.h"

namespace DB
{
class StorageCnchHive;

class IDirectoryLister
{
public:
    IDirectoryLister() = default;
    virtual ~IDirectoryLister() = default;

    virtual HiveFiles list(const HivePartitionPtr & partition) = 0;
};

class DiskDirectoryLister : public IDirectoryLister
{
public:
    explicit DiskDirectoryLister(const DiskPtr & disk_, IHiveFile::FileFormat format);

    HiveFiles list(const HivePartitionPtr & partition) override;

protected:
    DiskPtr disk;

private:
    IHiveFile::FileFormat file_format;
};

struct CnchHiveSettings;

namespace HiveUtil
{
/// Support HDFS/S3 now
DiskPtr getDiskFromURI(const String & sd_url, const ContextPtr & context, const CnchHiveSettings & settings);

String getPath(const String & path);
String getPathForListing(const String & path);
}

}

#endif
