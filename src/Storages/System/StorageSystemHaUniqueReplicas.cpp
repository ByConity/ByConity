#include <Storages/System/StorageSystemHaUniqueReplicas.h>

#include <common/getFQDNOrHostName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageHaUniqueMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/DNSResolver.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/typeid_cast.h>

namespace DB
{

StorageSystemHaUniqueReplicas::StorageSystemHaUniqueReplicas(const StorageID & table_id_) : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"engine", std::make_shared<DataTypeString>()},

        {"is_leader", std::make_shared<DataTypeUInt8>()},
        {"is_readonly", std::make_shared<DataTypeUInt8>()},
        {"is_session_expired", std::make_shared<DataTypeUInt8>()},
        {"status", std::make_shared<DataTypeString>()},
        {"cached_leader", std::make_shared<DataTypeString>()},
        {"zookeeper_path", std::make_shared<DataTypeString>()},
        {"replica_name", std::make_shared<DataTypeString>()},
        {"replica_path", std::make_shared<DataTypeString>()},
        {"absolute_delay", std::make_shared<DataTypeUInt64>()},

        {"checkpoint_lsn", std::make_shared<DataTypeUInt64>()},
        {"commit_lsn", std::make_shared<DataTypeUInt64>()},
        {"latest_lsn", std::make_shared<DataTypeUInt64>()},
        {"cluster_commit_lsn", std::make_shared<DataTypeUInt64>()},

        /// get from zookeeper
        {"cluster_latest_lsn", std::make_shared<DataTypeUInt64>()},
        {"total_replicas", std::make_shared<DataTypeUInt8>()},
        {"active_replicas", std::make_shared<DataTypeUInt8>()},

        {"shard_num", std::make_shared<DataTypeUInt32>()},
        {"replica_ip_address", std::make_shared<DataTypeString>()}
    }));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemHaUniqueReplicas::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    String local_ip_address = getIPOrFQDNOrHostName();
    UInt32 local_shard_num = 0;
    for (const auto & name_and_cluster : query_context->getClusters()->getContainer())
    {
        const ClusterPtr & cluster = name_and_cluster.second;
        const auto & shards_info = cluster->getShardsInfo();
        const auto & addresses_with_failover = cluster->getShardsAddresses();

        for (size_t shard_index = 0; shard_index < shards_info.size(); ++shard_index)
        {
            const auto & shard_addresses = addresses_with_failover[shard_index];

            for (size_t replica_index = 0; replica_index < shard_addresses.size(); ++replica_index)
            {
                const auto & address = shard_addresses[replica_index];
                if (!local_shard_num && DNSResolver::instance().resolveHost(address.host_name).toString() == local_ip_address) {
                    local_shard_num = shard_index + 1;
                    break;
                }
            }
        }
    }

    const auto access = query_context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);
    std::map<String, std::map<String, StoragePtr>> unique_tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        /// Check if database can contain replicated tables
        if (!db.second->canContainMergeTreeTables())
            continue;
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);
        for (auto iterator = db.second->getTablesIterator(query_context); iterator->isValid(); iterator->next())
        {
            const auto & table = iterator->table();
            if (!table)
                continue;

            if (!dynamic_cast<const StorageHaUniqueMergeTree *>(table.get()))
                continue;
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;
            unique_tables[db.first][iterator->name()] = table;
        }
    }

    /// Do you need columns that require a ZooKeeper request to compute.
    bool with_zk_fields = false;
    for (const auto & column_name : column_names)
    {
        if (column_name == "latest_lsn" || column_name == "total_replicas" || column_name == "active_replicas")
        {
            with_zk_fields = true;
            break;
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();
    MutableColumnPtr col_engine_mut = ColumnString::create();
    for (auto & db : unique_tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
            col_engine_mut->insert(table.second->getName());
        }
    }

    ColumnPtr col_database = std::move(col_database_mut);
    ColumnPtr col_table = std::move(col_table_mut);
    ColumnPtr col_engine = std::move(col_engine_mut);
    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
            {
                { col_database, std::make_shared<DataTypeString>(), "database" },
                { col_table, std::make_shared<DataTypeString>(), "table" },
                {col_engine, std::make_shared<DataTypeString>(), "engine"},
            };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, query_context);

        if (!filtered_block.rows())
            return {};

        col_database = filtered_block.getByName("database").column;
        col_table = filtered_block.getByName("table").column;
        col_engine = filtered_block.getByName("engine").column;
    }

    MutableColumns res_columns = metadata_snapshot->getSampleBlock().cloneEmptyColumns();
    for (size_t i = 0, tables_size = col_database->size(); i < tables_size; ++i)
    {
        String database = (*col_database)[i].safeGet<const String &>();
        String table = (*col_table)[i].safeGet<const String &>();
        StorageHaUniqueMergeTree::Status status;
        dynamic_cast<StorageHaUniqueMergeTree &>(
            *unique_tables[(*col_database)[i].safeGet<const String &>()][(*col_table)[i].safeGet<const String &>()])
            .getStatus(status, with_zk_fields);

        size_t col_num = 3;
        res_columns[col_num++]->insert(status.is_leader);
        res_columns[col_num++]->insert(status.is_readonly);
        res_columns[col_num++]->insert(status.is_session_expired);
        res_columns[col_num++]->insert(status.status);
        res_columns[col_num++]->insert(status.cached_leader);
        res_columns[col_num++]->insert(status.zookeeper_path);
        res_columns[col_num++]->insert(status.replica_name);
        res_columns[col_num++]->insert(status.replica_path);
        res_columns[col_num++]->insert(status.absolute_delay);
        res_columns[col_num++]->insert(status.checkpoint_lsn);
        res_columns[col_num++]->insert(status.commit_lsn);
        res_columns[col_num++]->insert(status.latest_lsn);
        res_columns[col_num++]->insert(status.cluster_commit_lsn);
        res_columns[col_num++]->insert(status.cluster_latest_lsn);
        res_columns[col_num++]->insert(status.total_replicas);
        res_columns[col_num++]->insert(status.active_replicas);
        res_columns[col_num++]->insert(local_shard_num);
        res_columns[col_num++]->insert(local_ip_address);
    }

    Block header = metadata_snapshot->getSampleBlock();

    Columns fin_columns;
    fin_columns.reserve(res_columns.size());

    for (auto & col : res_columns)
        fin_columns.emplace_back(std::move(col));

    fin_columns[0] = std::move(col_database);
    fin_columns[1] = std::move(col_table);
    fin_columns[2] = std::move(col_engine);

    UInt64 num_rows = fin_columns.at(0)->size();
    Chunk chunk(std::move(fin_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(metadata_snapshot->getSampleBlock(), std::move(chunk)));
}

}
