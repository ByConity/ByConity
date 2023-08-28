#pragma once

#include "Common/config.h"
#if USE_HIVE

#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"
#include "Disks/IDisk.h"

namespace DB
{

class IDirectoryLister
{
public:
    IDirectoryLister() = default;
    virtual ~IDirectoryLister() = default;

    virtual HiveFiles list() = 0;
};

class DiskDirectoryLister : public IDirectoryLister
{
public:
    DiskDirectoryLister(const HivePartitionPtr & partition_, const DiskPtr & disk_);

    HiveFiles list() override;

private:
    HivePartitionPtr partition;
    DiskPtr disk;
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
