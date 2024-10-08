#include <Catalog/Catalog.h>
#include <Parsers/queryToString.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTClusterByElement.h>
#include <Parsers/ASTCreateQuery.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemCnchViewTables.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Status.h>
#include <common/logger_useful.h>
// #include <Parsers/ASTClusterByElement.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StorageSystemCnchViewTables::StorageSystemCnchViewTables(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
        {
            {"database", std::make_shared<DataTypeString>()},
            {"table", std::make_shared<DataTypeString>()},
            {"uuid", std::make_shared<DataTypeUUID>()},
            {"vw_name", std::make_shared<DataTypeString>()},
            {"definition", std::make_shared<DataTypeString>()},
            {"base_table_databases", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
            {"base_table_tables", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
            {"dependent_table_databases", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
            {"dependent_table_tables", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
            {"target_table_database", std::make_shared<DataTypeString>()},
            {"target_table_table", std::make_shared<DataTypeString>()},
            {"partition_diffs", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
            {"latest_visible_partitions", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
            {"previous_partitions", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
            {"refresh_type", std::make_shared<DataTypeString>()},
            {"refresh_start_time", std::make_shared<DataTypeString>()},
            {"refresh_interval", std::make_shared<DataTypeString>()},
            {"is_refeshable", std::make_shared<DataTypeUInt8>()},
        }));
    setInMemoryMetadata(storage_metadata);
}

std::optional<StorageID> parseStorageIDFromWhere(SelectQueryInfo & query_info)
{
    ASTPtr where_expression = query_info.query->as<ASTSelectQuery &>().where();
    std::map<String,String> predicates;
    collectWhereClausePredicate(where_expression, predicates);

    std::optional<StorageID> storage_id;
    if (predicates.count("database") && predicates.count("name"))
        storage_id = StorageID(predicates["database"], predicates["name"]);

    return storage_id;
}

Pipe StorageSystemCnchViewTables::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    Catalog::CatalogPtr cnch_catalog = context->getCnchCatalog();

    if (context->getServerType() != ServerType::cnch_server || !cnch_catalog)
        throw Exception("Table system.cnch_tables_history only support cnch_server", ErrorCodes::LOGICAL_ERROR);

    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    Block header;

    std::vector<UInt8> columns_mask(sample_block.columns());
    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.count(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            header.insert(sample_block.getByPosition(i));
        }
    }

    Catalog::Catalog::DataModelTables table_models;

    auto required_table = parseStorageIDFromWhere(query_info);

    if (required_table)
    {
        auto table_id = cnch_catalog->getTableIDByName(required_table->getDatabaseName(), required_table->getTableName());
        if (table_id)
            table_models = cnch_catalog->getTablesByIDs(std::vector<std::shared_ptr<Protos::TableIdentifier>>{std::move(table_id)});
    }
    else
        table_models = cnch_catalog->getAllTables();

    Block block_to_filter;

    /// Add `database` column.
    MutableColumnPtr database_column_mut = ColumnString::create();
    /// Add `name` column.
    MutableColumnPtr name_column_mut = ColumnString::create();
    /// Add `uuid` column
    MutableColumnPtr uuid_column_mut = ColumnUUID::create();
    /// ADD 'index' column
    MutableColumnPtr index_column_mut = ColumnUInt64::create();

    for (size_t i = 0; i < table_models.size(); i++)
    {
        database_column_mut->insert(table_models[i].database());
        name_column_mut->insert(table_models[i].name());
        uuid_column_mut->insert(RPCHelpers::createUUID(table_models[i].uuid()));
        index_column_mut->insert(i);
    }

    block_to_filter.insert(ColumnWithTypeAndName(std::move(database_column_mut), std::make_shared<DataTypeString>(), "database"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(name_column_mut), std::make_shared<DataTypeString>(), "name"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(uuid_column_mut), std::make_shared<DataTypeUUID>(), "uuid"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(index_column_mut), std::make_shared<DataTypeUInt64>(), "index"));

    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

    if (!block_to_filter.rows())
        return Pipe(std::make_shared<NullSource>(std::move(header)));

    ColumnPtr filtered_index_column = block_to_filter.getByName("index").column;

    MutableColumns res_columns = header.cloneEmptyColumns();

    for (size_t i = 0; i<filtered_index_column->size(); i++)
    {
        auto table_model = table_models[(*filtered_index_column)[i].get<UInt64>()];
        if (Status::isDeleted(table_model.status()))
            continue;

        Array base_table_databases;
        Array base_table_tables;
        Array dependent_table_databases;
        Array dependent_table_tables;
        String target_table_database = "";
        String target_table_table = "";
        Array partition_diffs;
        Array latest_visible_partitions;
        Array previous_partitions_array;
        UInt8 is_refeshable = false;

        StoragePtr storage_ptr = nullptr;
        StorageMaterializedView * mv = nullptr;

        try
        {
            storage_ptr = cnch_catalog->tryGetTableByUUID(*context, UUIDHelpers::UUIDToString(RPCHelpers::createUUID(table_model.uuid())), TxnTimestamp::maxTS());
            mv = dynamic_cast<StorageMaterializedView*>(storage_ptr.get());
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("StorageSystemCnchViewTables"));
            continue;
        }
        if (!mv)
            continue;

        auto & refresh_schedule = mv->getRefreshSchedule();
        size_t col_num = 0;
        size_t src_index = 0;
        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_model.database());

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_model.name());

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(RPCHelpers::createUUID(table_model.uuid()));

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_model.vw_name());

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_model.definition());

        if (columns_mask[src_index] || columns_mask[src_index + 1])
        {
            auto & table_ids = mv->getInMemoryMetadataPtr()->select.base_table_ids;
            for (auto & table_id : table_ids)
            {
                base_table_databases.push_back(table_id->getDatabaseName());
                base_table_tables.push_back(table_id->getTableName());
            }
        }

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(base_table_databases);

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(base_table_tables);

        if (columns_mask[src_index] || columns_mask[src_index + 1])
        {
            if (refresh_schedule.async())
            {
                ContextMutablePtr query_context = Context::createCopy(context);
                mv->validatePartitionBased(query_context);
                auto table_ids = mv->getDependBaseTables();
                for (auto & table_id : table_ids)
                {
                    dependent_table_databases.push_back(table_id.first.getDatabaseName());
                    dependent_table_tables.push_back(table_id.first.getTableName());
                }
            }
        }

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(dependent_table_databases);

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(dependent_table_tables);

        if (columns_mask[src_index] || columns_mask[src_index + 1])
        {
            target_table_database = mv->getTargetDatabaseName();
            target_table_table = mv->getTargetTableName();
        }

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(target_table_database);
        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(target_table_table);

        PartitionDiffPtr partition_diff = std::make_shared<PartitionDiff>();
        VersionPartContainerPtrs latest_versioned_partitions;
        if (columns_mask[src_index] || columns_mask[src_index + 1])
        {
            if (refresh_schedule.async())
            {
                ContextMutablePtr query_context = Context::createCopy(context);
                mv->validateAndSyncBaseTablePartitions(partition_diff, latest_versioned_partitions, query_context);
            }
        }

        if (columns_mask[src_index++])
        {
            if (refresh_schedule.async())
            {
                String add_partitions_string = "add partitions: ";
                for (auto & add_partition : partition_diff->add_partitions)
                {
                    auto temp = fmt::format("{} ({}), ", add_partitions_string,
                        mv->versionPartitionToString(*add_partition));
                    add_partitions_string = temp;
                }
                partition_diffs.push_back(add_partitions_string);

                String drop_partitions_string = "drop partitions: ";
                for (auto & drop_partition : partition_diff->drop_partitions)
                {
                    auto temp = fmt::format("{} ({}), ", drop_partitions_string,
                        mv->versionPartitionToString(*drop_partition));
                    drop_partitions_string = temp;
                }

                partition_diffs.push_back(drop_partitions_string);
            }
            res_columns[col_num++]->insert(partition_diffs);
        }

        if (columns_mask[src_index++])
        {
            if (refresh_schedule.async())
            {
                for (auto & table_partitions : latest_versioned_partitions)
                {
                    StorageID storage_id = RPCHelpers::createStorageID(table_partitions->storage_id());
                    String single_table_string = fmt::format("{}: ", storage_id.getFullTableName());
                    for (const auto & partition : table_partitions->versioned_partition())
                    {
                        auto temp = fmt::format("{} {},", single_table_string,
                            mv->versionPartitionToString(partition));
                        single_table_string = temp;
                    }
                    latest_visible_partitions.push_back(single_table_string);
                }
            }
            res_columns[col_num++]->insert(latest_visible_partitions);
        }

        if (columns_mask[src_index++])
        {
            if (refresh_schedule.async())
            {
                ContextMutablePtr query_context = Context::createCopy(context);
                VersionPartContainerPtrs previous_partitions = query_context->getCnchCatalog()->getMvBaseTables(UUIDHelpers::UUIDToString(mv->getStorageUUID()));
                for (auto & table_partitions : previous_partitions)
                {
                    StorageID storage_id = RPCHelpers::createStorageID(table_partitions->storage_id());
                    String single_table_string = fmt::format("{}: ", storage_id.getFullTableName());
                    for (const auto & partition : table_partitions->versioned_partition())
                    {
                        auto temp = fmt::format("{} {},", single_table_string,
                            mv->versionPartitionToString(partition));
                        single_table_string = temp;
                    }
                    previous_partitions_array.push_back(single_table_string);
                }
            }
            res_columns[col_num++]->insert(previous_partitions_array);
        }

        if (columns_mask[src_index++])
        {
            res_columns[col_num++]->insert(toString(refresh_schedule.kind));
        }

        if (columns_mask[src_index++])
        {
            String start_time = "";
            if (refresh_schedule.async())
            {
                start_time = refresh_schedule.getStartTime();
            }
            res_columns[col_num++]->insert(start_time);
        }

        if (columns_mask[src_index++])
        {
            if (refresh_schedule.async())
                res_columns[col_num++]->insert(refresh_schedule.time_interval.toIntervalString());
            else
                res_columns[col_num++]->insert("");
        }

        if (columns_mask[src_index])
        {
            // is_refeshable = mv->isRefreshable(false);
        }

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(is_refeshable);
    }

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
}

}
