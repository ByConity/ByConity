#include <Disks/DiskHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

String getDiskNameForPathId(const VolumePtr& volume, UInt32 path_id)
{
    if (path_id == 0)
    {
        return volume->getDefaultDisk()->getName();
    }
    else
    {
        return "HDFS/" + toString(path_id);
    }
}

String getDiskNameForPathId(const StoragePolicyPtr& storage_policy, UInt32 path_id)
{
    return getDiskNameForPathId(storage_policy->getVolume(0), path_id);
}

DiskPtr getDiskForPathId(const StoragePolicyPtr& storage_policy, UInt32 path_id)
{
    VolumePtr remote_volume = storage_policy->getVolume(0);
    String disk_name = getDiskNameForPathId(remote_volume, path_id);
    DiskPtr disk = storage_policy->getDiskByName(disk_name);
    if (disk == nullptr)
        throw Exception("Disk " + disk_name + " not found in " + storage_policy->getName(),
            ErrorCodes::INVALID_CONFIG_PARAMETER);
    return disk;
}

}
