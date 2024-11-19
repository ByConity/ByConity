#pragma once

#include <Common/config.h>
#if USE_HIVE

#include <Disks/IDisk.h>
#include <Storages/DataLakes/ScanInfo/FileScanInfo.h>
#include <Storages/DataLakes/ScanInfo/ILakeScanInfo.h>
#include <Storages/Hive/HivePartition.h>

namespace DB
{
class StorageCnchHive;

class IDirectoryLister
{
public:
    IDirectoryLister() = default;
    virtual ~IDirectoryLister() = default;

    virtual LakeScanInfos list(const HivePartitionPtr & partition) = 0;
};

class DiskDirectoryLister : public IDirectoryLister
{
public:
    explicit DiskDirectoryLister(const DiskPtr & disk_, ILakeScanInfo::StorageType storage_type_, FileScanInfo::FormatType format_type_);

    LakeScanInfos list(const HivePartitionPtr & partition) override;

protected:
    DiskPtr disk;
    ILakeScanInfo::StorageType storage_type;
    FileScanInfo::FormatType format_type;
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
