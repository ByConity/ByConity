#include <memory>
#include <hive_metastore_types.h>
#include <IO/S3Common.h>
#include <IO/S3/Credentials.h>
#include <IO/S3/CustomCRTHttpClient.h>
#include <Storages/Hive/Metastore/GlueMetastore.h>
#include <Storages/Hive/Metastore/MetastoreConvertUtils.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/glue/GlueClient.h>
#include <aws/glue/model/GetColumnStatisticsForPartitionRequest.h>
#include <aws/glue/model/GetColumnStatisticsForPartitionResult.h>
#include <aws/glue/model/GetColumnStatisticsForTableRequest.h>
#include <aws/glue/model/GetDatabasesRequest.h>
#include <aws/glue/model/GetDatabasesResult.h>
#include <Common/Exception.h>
#include "Core/Types.h"
#include "ExternalCatalog/IExternalCatalogMgr.h"
#include "aws/glue/model/GetPartitionsRequest.h"
#include "aws/glue/model/GetTableRequest.h"
#include "aws/glue/model/GetTableResult.h"
#include "aws/glue/model/GetTablesRequest.h"
#include <aws/glue/GlueErrors.h>

#include <Storages/Hive/Metastore/MetastoreConvertUtils.h>
#include <Parsers/formatTenantDatabaseName.h>
namespace DB::ErrorCodes
{
extern const int GLUE_CATALOG_RPC_ERROR;
}

#define THROW_IF_FAIL(outcome) \
    do \
    { \
        if (!(outcome).IsSuccess()) \
        { \
            std::stringstream ss; \
            ss << (outcome).GetError(); \
            throw DB::Exception(DB::ErrorCodes::GLUE_CATALOG_RPC_ERROR, "Glue Catalog Error {}", ss.str()); \
        } \
    } while (0)
namespace DB
{
namespace GlueModel = Aws::Glue::Model;
GlueMetastoreClient::GlueMetastoreClient(
    const Aws::Auth::AWSCredentials & credentials, const std::shared_ptr<Aws::Client::ClientConfiguration> & client_config, const GlueClientAuxParams & _aux)
    : cfg(client_config), aux(_aux), client(credentials, *client_config)
{
}

Strings GlueMetastoreClient::getAllDatabases()
{
    GlueModel::GetDatabasesRequest request;
    std::string next_token;
    Strings all_db;
    do
    {
        GlueModel::GetDatabasesOutcome outcome = client.GetDatabases(request.WithCatalogId(aux.catalog_id).WithNextToken(next_token));
        THROW_IF_FAIL(outcome);
        auto & result = outcome.GetResult();
        next_token = result.GetNextToken();
        for (const auto & db : result.GetDatabaseList())
        {
            all_db.emplace_back(db.GetName());
        }
    } while (!next_token.empty());
    return all_db;
}

Strings GlueMetastoreClient::getAllTables(const String & db_name)
{
    GlueModel::GetTablesRequest request;
    std::string next_token;
    Strings all_tbl;

    do
    {
        GlueModel::GetTablesOutcome outcome
            = client.GetTables(request.WithCatalogId(aux.catalog_id).WithNextToken(next_token).WithDatabaseName(getOriginalDatabaseName(db_name)));
        THROW_IF_FAIL(outcome);
        auto & result = outcome.GetResult();
        next_token = result.GetNextToken();
        for (const auto & tbl : result.GetTableList())
        {
            all_tbl.emplace_back(tbl.GetName());
        }
    } while (!next_token.empty());
    return all_tbl;
}

std::shared_ptr<ApacheHive::Table> GlueMetastoreClient::getTable(const String & db_name, const String & table_name)
{
    GlueModel::GetTableRequest request;
    GlueModel::GetTableOutcome outcome
        = client.GetTable(request.WithCatalogId(aux.catalog_id).WithDatabaseName(getOriginalDatabaseName(db_name)).WithName(table_name));
    THROW_IF_FAIL(outcome);
    auto glue_table = outcome.GetResult().GetTable();
    auto hive_table = std::make_shared<ApacheHive::Table>();
    hive_table->dbName = db_name;
    MetastoreConvertUtils::convertTable(glue_table, *hive_table);
    return hive_table;
}

bool GlueMetastoreClient::isTableExist(const String & db_name, const String & table_name)
{
    GlueModel::GetTableRequest request;
    GlueModel::GetTableOutcome outcome
        = client.GetTable(request.WithCatalogId(aux.catalog_id).WithDatabaseName(getOriginalDatabaseName(db_name)).WithName(table_name));
    if(outcome.IsSuccess()) return true;
    auto err = outcome.GetError().GetErrorType();
    if(err == Aws::Glue::GlueErrors::ENTITY_NOT_FOUND){
        return false;
    }
    THROW_IF_FAIL(outcome);
    return false;
}

std::vector<ApacheHive::Partition>
GlueMetastoreClient::getPartitionsByFilter(const String & db_name, const String & table_name, const String & filter)
{
    GlueModel::GetPartitionsRequest request;
    std::vector<ApacheHive::Partition> hive_partitions;
    std::string next_token;

    do
    {
        GlueModel::GetPartitionsOutcome outcome = client.GetPartitions(
            request.WithCatalogId(aux.catalog_id).WithDatabaseName(getOriginalDatabaseName(getOriginalDatabaseName(db_name))).WithTableName(table_name).WithExpression(filter));
        THROW_IF_FAIL(outcome);
        auto & result = outcome.GetResult();
        next_token = result.GetNextToken();
        for (const auto & gp : result.GetPartitions())
        {
            hive_partitions.emplace_back(MetastoreConvertUtils::convertPartition(gp));
        }
    } while (!next_token.empty());
    return hive_partitions;
}

// TODO(renming):: Merge all partitions.
std::optional<TableStatistics> GlueMetastoreClient::getTableStats(
    [[maybe_unused]] const String & db_name,
    [[maybe_unused]] const String & table_name,
    [[maybe_unused]] const Strings & col_names,
    [[maybe_unused]] const bool merge_all_partition)
{
    auto table = getTable(db_name, table_name);
    if (table->partitionKeys.empty() || !merge_all_partition)
    {
        GlueModel::GetColumnStatisticsForTableRequest request;
        for (const auto & col : col_names)
        {
            request.AddColumnNames(col);
        }
        GlueModel::GetColumnStatisticsForTableOutcome outcome
            = client.GetColumnStatisticsForTable(request.WithCatalogId(aux.catalog_id).WithDatabaseName(getOriginalDatabaseName(db_name)).WithTableName(table_name));
        THROW_IF_FAIL(outcome);
        auto & result = outcome.GetResult();
        if (table->__isset.parameters && table->parameters.contains("numRows"))
        {
            ApacheHive::TableStatsResult hive_stat;
            for (const auto & stat : result.GetColumnStatisticsList())
            {
                hive_stat.tableStats.emplace_back(MetastoreConvertUtils::convertTableStatistics(stat));
            }
            return MetastoreConvertUtils::convertHiveStats({std::stol(table->parameters.at("numRows")), hive_stat});
        }
        else
        {
            return MetastoreConvertUtils::convertHiveStats({0, {}});
        }
    }

    auto partitions = getPartitionsByFilter(db_name, table_name, "");
    Strings partition_keys;
    std::vector<Strings> partition_values;

    partition_keys.reserve(table->partitionKeys.size());
    for (const auto & field : table->partitionKeys)
    {
        partition_keys.emplace_back(field.name);
    }

    partition_values.reserve(partitions.size());
    for (const auto & p : partitions)
    {
        partition_values.emplace_back(p.values);
    }

    auto partition_stats = getPartitionStats(db_name, table_name, col_names, partition_keys, partition_values);

    auto stats = MetastoreConvertUtils::merge_partition_stats(*table, partitions, partition_stats);

    return MetastoreConvertUtils::convertHiveStats(stats);
}

// TODO(renming):: parallelize it
ApacheHive::PartitionsStatsResult GlueMetastoreClient::getPartitionStats(
    const String & db_name,
    const String & table_name,
    const Strings & col_names,
    const Strings & partition_keys,
    const std::vector<Strings> & partition_vals)
{
    GlueModel::GetColumnStatisticsForPartitionRequest req;

    ApacheHive::PartitionsStatsResult hive_stats;
    for (const auto & vals : partition_vals)
    {
        GlueModel::GetColumnStatisticsForPartitionOutcome outcome = client.GetColumnStatisticsForPartition(req.WithCatalogId(aux.catalog_id)
                                                                                                               .WithDatabaseName(getOriginalDatabaseName(db_name))
                                                                                                               .WithTableName(table_name)
                                                                                                               .WithColumnNames(col_names)
                                                                                                               .WithPartitionValues(vals));
        THROW_IF_FAIL(outcome);

        auto & result = outcome.GetResult();
        std::vector<ApacheHive::ColumnStatisticsObj> hive_column_stats;
        for (const auto & stat : result.GetColumnStatisticsList())
        {
            auto hive_stat_one_partition = MetastoreConvertUtils::convertTableStatistics(stat);
            hive_column_stats.emplace_back(hive_stat_one_partition);
        }
        auto partition_str = MetastoreConvertUtils::concatPartitionValues(partition_keys, vals);
        hive_stats.partStats.emplace(partition_str, std::move(hive_column_stats));
    }
    return hive_stats;
}


GlueMetastoreClientFactory & GlueMetastoreClientFactory::instance()
{
    static GlueMetastoreClientFactory factory;
    return factory;
}


namespace GlueConfigKey
{
    String aws_glue_catalog_id = "aws.glue.catalog_id";
    String aws_glue_ak = "aws.glue.access_key";
    String aws_glue_sk = "aws.glue.secret_key";
    String aws_glue_region = "aws.glue.region";
    String aws_glue_endpoint = "aws.glue.endpoint";
    String aws_glue_use_instance_profile = "aws.glue.use_instance_profile";
}

GlueMetastoreClientPtr GlueMetastoreClientFactory::getOrCreate(const ExternalCatalog::PlainConfigsPtr & cfg)
{
    auto catalog_id = cfg->getString(GlueConfigKey::aws_glue_catalog_id);
    std::lock_guard lock(mutex);
    auto it = clients.find(catalog_id);
    if (it != clients.end())
    {
        return it->second;
    }

    auto glue_ak = cfg->getString(GlueConfigKey::aws_glue_ak, "");
    auto glue_sk = cfg->getString(GlueConfigKey::aws_glue_sk, "");
    auto region = cfg->getString(GlueConfigKey::aws_glue_region, "");
    auto endpoint = cfg->getString(GlueConfigKey::aws_glue_endpoint);
    auto use_instance_profile = cfg->getBool(GlueConfigKey::aws_glue_use_instance_profile, false);

    // TODO(renming):: use config in catalog.
    std::shared_ptr<Aws::Client::ClientConfiguration> conf
        = DB::S3::ClientFactory::instance().createClientConfiguration(region, DB::RemoteHostFilter(), 3, 10000, 1024, false);
    conf->endpointOverride = endpoint;
    conf->region = region;


    Aws::Auth::AWSCredentials aws_credential(glue_ak, glue_sk);
    DB::S3::S3CredentialsProviderChain credential_chain(conf, aws_credential, DB::S3::CredentialsConfiguration{use_instance_profile, false});
    auto glue_credential = credential_chain.GetAWSCredentials();

    GlueClientAuxParams params{.catalog_id = catalog_id};
    std::shared_ptr<GlueMetastoreClient> client = std::make_shared<GlueMetastoreClient>(glue_credential, conf, params);
    clients.emplace(catalog_id, client);
    return client;
}


GlueMetastoreClientPtr GlueMetastoreClientFactory::getOrCreate([[maybe_unused]]const std::string & name, const std::shared_ptr<CnchHiveSettings> & settings)
{
    auto catalog_id = settings->aws_glue_catalog_id.value;
    std::lock_guard lock(mutex);
    auto it = clients.find(catalog_id);
    if (it != clients.end())
    {
        return it->second;
    }

    auto glue_ak = settings->aws_glue_ak_id.value;
    auto glue_sk = settings->aws_glue_ak_secret.value;
    auto region =  settings->aws_glue_ak_secret.value;
    auto endpoint = settings->aws_glue_endpoint.value;
    auto use_instance_profile = settings->aws_glue_use_instance_profile.value;

    // TODO(renming):: use config in catalog.
    std::shared_ptr<Aws::Client::ClientConfiguration> conf
        = DB::S3::ClientFactory::instance().createClientConfiguration(region, DB::RemoteHostFilter(), 3, 10000, 1024, false);
    conf->endpointOverride = endpoint;
    conf->region = region;


    Aws::Auth::AWSCredentials aws_credential(glue_ak, glue_sk);
    DB::S3::S3CredentialsProviderChain credential_chain(conf, aws_credential, DB::S3::CredentialsConfiguration{use_instance_profile, false});
    auto glue_credential = credential_chain.GetAWSCredentials();

    GlueClientAuxParams params{.catalog_id = catalog_id};
    std::shared_ptr<GlueMetastoreClient> client = std::make_shared<GlueMetastoreClient>(glue_credential, conf, params);
    clients.emplace(catalog_id, client);
    return client;
}

}

