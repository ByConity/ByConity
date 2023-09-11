#pragma once
#include "Common/config.h"
#if USE_HIVE

#include "Storages/Hive/DirectoryLister.h"
#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"

namespace DB
{

class HudiCowDirectoryLister : public DiskDirectoryLister
{
public:
    explicit HudiCowDirectoryLister(const DiskPtr & disk);

    HiveFiles list(const HivePartitionPtr & partition) override;
};

}

#endif
