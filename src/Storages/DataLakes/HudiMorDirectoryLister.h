#pragma once
#include "Common/config.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include "Storages/Hive/DirectoryLister.h"
#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"

namespace DB
{
class JNIMetaClient;
class StorageCnchHudi;

class HudiMorDirectoryLister : public DB::DiskDirectoryLister
{
public:
    explicit HudiMorDirectoryLister(
        const DiskPtr & disk, const String & base_path, const StorageCnchHudi & hudi_table);
    HiveFiles list(const HivePartitionPtr & partition) override;

private:
    std::shared_ptr<JNIMetaClient> jni_client;
    std::unordered_map<String, String> table_properties;
    const String & basePath();
};

}

#endif
