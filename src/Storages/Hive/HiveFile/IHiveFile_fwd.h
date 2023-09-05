#pragma once

#include "Common/config.h"
#if USE_HIVE

#include <memory>

namespace DB
{
struct HivePartition;
using HivePartitionPtr = std::shared_ptr<HivePartition>;
using HivePartitions = std::vector<HivePartitionPtr>;

class IHiveFile;
using HiveFilePtr = std::shared_ptr<IHiveFile>;
using HiveFiles = std::vector<HiveFilePtr>;

struct HiveFilesSerDe;

}

#endif
