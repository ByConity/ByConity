#pragma once
#include <Common/config.h>
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <Storages/DataLakes/ScanInfo/ILakeScanInfo.h>
#include <Storages/Hive/HivePartition.h>
#include <Storages/Hive/Metastore/IMetaClient.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class JNIMetaClient;

class JNIHiveMetastoreClient : public IMetaClient
{
public:
    explicit JNIHiveMetastoreClient(std::shared_ptr<JNIMetaClient> jni_metaclient, const std::unordered_map<String, String> & properties_);

    Strings getAllDatabases() override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getAllDatabases is not implement"); }

    Strings getAllTables(const String &) override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getAllDatabases is not implement"); }

    std::shared_ptr<ApacheHive::Table> getTable(const String & db_name, const String & table_name) override;

    bool isTableExist(const String & db_name, const String & table_name) override;

    std::vector<ApacheHive::Partition>
    getPartitionsByFilter(const String & db_name, const String & table_name, const String & filter) override;

    std::optional<TableStatistics> getTableStats(const String &, const String &, const Strings &, bool) override { return {}; }

    // ApacheHive::TableStatsResult getPartitionedTableStats(const String & db_name, const String & table_name, const Strings& col_names, const std::vector<ApacheHive::Partition>& partitions ) override;
    ApacheHive::PartitionsStatsResult
    getPartitionStats(const String &, const String &, const Strings &, const Strings &, const std::vector<Strings> &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getAllDatabases is not implement");
    }

    LakeScanInfos getFilesInPartition(const HivePartitions & partitions, size_t min_split_num, size_t max_threads);

    std::unordered_map<String, String> & getProperties() { return properties; }

private:
    std::shared_ptr<JNIMetaClient> jni_metaclient;
    std::unordered_map<String, String> properties;
};

}
#endif
