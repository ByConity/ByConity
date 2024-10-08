#pragma once
#include "Common/config.h"
#include "Core/Types.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS
#include <hive_metastore_types.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/Hive/Metastore/IMetaClient.h>

#include <unordered_map>
#include <jni/JNIMetaClient.h>
#include <Common/PoolBase.h>
namespace DB
{

// class JNIMetaClient;
class JNILfMetastore : public IMetaClient
{
public:
    static std::unordered_map<String, String> buildPropertiesFromHiveSettigns(const CnchHiveSettings & settings);

    explicit JNILfMetastore(const std::unordered_map<String, String> & properties);

    Strings getAllDatabases() override;

    Strings getAllTables(const String & db_name) override;


    std::shared_ptr<ApacheHive::Table> getTable(const String & db_name, const String & table_name) override;

    bool isTableExist(const String & db_name, const String & table_name) override;

    std::vector<ApacheHive::Partition>
    getPartitionsByFilter(const String & db_name, const String & table_name, const String & filter) override;


    std::optional<TableStatistics> getTableStats(const String &, const String &, const Strings &, bool) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getTableStats is not implement");
    }

    ApacheHive::PartitionsStatsResult
    getPartitionStats(const String &, const String &, const Strings &, const Strings &, const std::vector<Strings> &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getPartitionStats is not implement");
    }


private:
    std::unique_ptr<JNIMetaClient> jni_client;
    std::unordered_map<String, String> properties;
};


using JNILfMetastoreClientPtr = std::shared_ptr<JNILfMetastore>;
class JNILfMetastoreClientFactory final : private boost::noncopyable
{
public:
    static JNILfMetastoreClientFactory & instance();

    JNILfMetastoreClientPtr getOrCreate(const String & name, const std::shared_ptr<CnchHiveSettings> & settings);

private:
    // static JNILfMetastoreClientPtr createThriftHiveMetastoreClient(const String & name, const std::shared_ptr<CnchHiveSettings> & settings);

    std::mutex mutex;
    std::map<String, JNILfMetastoreClientPtr> clients;
};

}

#endif
