#include <Catalog/Catalog.h>
#include <Storages/System/StorageSystemCnchManifestList.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Parsers/queryToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

StorageSystemCnchManifestList::StorageSystemCnchManifestList(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
        {
            {"database", std::make_shared<DataTypeString>()},
            {"table", std::make_shared<DataTypeString>()},
            {"uuid", std::make_shared<DataTypeUUID>()},
            {"version", std::make_shared<DataTypeUInt64>()},
            {"txn_ids", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
            {"checkpoint_version", std::make_shared<DataTypeUInt8>()},
        }));
    setInMemoryMetadata(storage_metadata);
}


Pipe StorageSystemCnchManifestList::read(
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
        throw Exception("Table system.cnch_manifest_list only support cnch_server", ErrorCodes::LOGICAL_ERROR);
    

    ASTPtr where_condition = query_info.query->as<const ASTSelectQuery &>().where();
    std::vector<std::map<String,Field>> predicates = collectWhereORClausePredicate(where_condition, context);
    String database_name = "", table_name = "", uuid = "";
    StoragePtr storage = nullptr;

    for (auto & value_by_column_names : predicates)
    {
        if (value_by_column_names.count("database") && value_by_column_names["database"].getType() == Field::Types::String)
            database_name = value_by_column_names["database"].get<String>();
        if (value_by_column_names.count("table") && value_by_column_names["table"].getType() == Field::Types::String)
            table_name = value_by_column_names["table"].get<String>();
        if (value_by_column_names.count("uuid") && value_by_column_names["uuid"].getType() == Field::Types::String)
            uuid = value_by_column_names["uuid"].get<String>();
    }

    if (database_name.size() && table_name.size())
        storage = cnch_catalog->tryGetTable(*context, database_name, table_name);
    else if (uuid.size())
        storage = cnch_catalog->tryGetTableByUUID(*context, uuid, TxnTimestamp::maxTS());
    else
        throw Exception("You should specify the table by its name or uuid when query from system.cnch_manifest_list.", ErrorCodes::INCORRECT_QUERY);

    if (!storage)
        throw Exception("Cannot get any table from query condition " + queryToString(where_condition), ErrorCodes::INCORRECT_QUERY);

    auto table_versions = cnch_catalog->getAllTableVersions(storage->getStorageUUID());

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

    MutableColumns res_columns = header.cloneEmptyColumns();

    for (auto table_version : table_versions)
    {
        size_t col_num = 0;
        size_t src_index = 0;

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(storage->getDatabaseName());
        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(storage->getTableName());
        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(storage->getStorageUUID());
        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_version->version());

        if (columns_mask[src_index++])
        {
            Array txn_ids;
            const auto & repeated_txn_id = table_version->txn_ids();
            txn_ids.reserve(repeated_txn_id.size());
            for (const auto & txn_id : repeated_txn_id)
                txn_ids.push_back(txn_id);
            
            res_columns[col_num++]->insert(txn_ids);
        }

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_version->checkpoint());
    }

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
}

}
