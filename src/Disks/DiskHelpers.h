#pragma once

#include "IVolume.h"
#include "StoragePolicy.h"
namespace DB
{

String getDiskNameForPathId(const VolumePtr& volume, UInt32 path_id);
String getDiskNameForPathId(const StoragePolicyPtr& storage_policy, UInt32 path_id);
DiskPtr getDiskForPathId(const StoragePolicyPtr& storage_policy, UInt32 path_id);

}
