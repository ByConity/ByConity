#pragma once
#include "Common/config.h"
#if USE_HIVE

#include <Storages/DataLakes/ScanInfo/ILakeScanInfo.h>
#include <Storages/Hive/DirectoryLister.h>

namespace DB
{

class HudiCowDirectoryLister : public DiskDirectoryLister
{
public:
    explicit HudiCowDirectoryLister(const DiskPtr & disk);

    LakeScanInfos list(const HivePartitionPtr & partition) override;
};

}

#endif
