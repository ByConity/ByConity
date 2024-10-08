#pragma once

#include <aws/core/client/ClientConfiguration.h>
#include "ExternalCatalog/IExternalCatalogMgr.h"
#include "Storages/Hive/Metastore/IMetaClient.h"
#if USE_HIVE
#include <aws/glue/GlueClient.h>
#include <boost/noncopyable.hpp>
namespace DB
{

struct GlueClientAuxParams
{
    std::string catalog_id;
};
class GlueMetastoreClient : public IMetaClient
{
public:
    explicit GlueMetastoreClient(
        const Aws::Auth::AWSCredentials & credentials,
        const std::shared_ptr<Aws::Client::ClientConfiguration> & client_config,
        const GlueClientAuxParams & _aux);

    Strings getAllDatabases() override;

    Strings getAllTables(const String & db_name) override;

    std::shared_ptr<ApacheHive::Table> getTable(const String & db_name, const String & table_name) override;

    bool isTableExist(const String & db_name, const String & table_name) override;

    std::vector<ApacheHive::Partition>
    getPartitionsByFilter(const String & db_name, const String & table_name, const String & filter) override;

    std::optional<TableStatistics> getTableStats(
        const String & db_name, const String & table_name, const Strings & col_names, bool merge_all_partition) override;

    ApacheHive::PartitionsStatsResult getPartitionStats(
        const String & db_name,
        const String & table_name,
        const Strings & col_names,
        const Strings & partition_keys,
        const std::vector<Strings> & partition_vals) override;

private:
    std::shared_ptr<Aws::Client::ClientConfiguration> cfg;
    GlueClientAuxParams aux;
    Aws::Glue::GlueClient client;
};

using GlueMetastoreClientPtr = std::shared_ptr<GlueMetastoreClient>;
class GlueMetastoreClientFactory final : private boost::noncopyable
{
public:
    static GlueMetastoreClientFactory & instance();

    GlueMetastoreClientPtr getOrCreate(const ExternalCatalog::PlainConfigsPtr & configs);
    GlueMetastoreClientPtr getOrCreate(const std::string & name, const std::shared_ptr<CnchHiveSettings> & settings);
private:
    std::mutex mutex;
    std::map<String, GlueMetastoreClientPtr> clients;
};

namespace GlueConfigKey
{
    extern String aws_glue_catalog_id;
    extern String aws_glue_ak;
    extern String aws_glue_sk;
    extern String aws_glue_region;
    extern String aws_glue_endpoint;
    extern String aws_glue_use_instance_profile;
}

}

#endif
