#pragma once
#include <Common/config.h>
#include "Storages/DataLakes/ScanInfo/ILakeScanInfo.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <Storages/Hive/DirectoryLister.h>

namespace DB
{
class JNIMetaClient;
class StorageCnchHudi;

class HudiMorDirectoryLister : public IDirectoryLister
{
public:
    explicit HudiMorDirectoryLister(const String & base_path, const StorageCnchHudi & hudi_table);
    LakeScanInfos list(const HivePartitionPtr & partition) override;

private:
    std::shared_ptr<JNIMetaClient> jni_client;
    std::unordered_map<String, String> table_properties;
    const String & basePath();
};

}

#endif
