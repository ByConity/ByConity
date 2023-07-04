#include <Storages/System/StorageSystemCloudTables.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>
#include <CloudServices/CnchWorkerResource.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeUUID.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>


namespace DB
{

StorageSystemCloudTables::StorageSystemCloudTables(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"txn_id", std::make_shared<DataTypeUInt64>()},
        {"engine", std::make_shared<DataTypeString>()},
        {"data_paths", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"create_time", std::make_shared<DataTypeDateTime>()},
        {"partition_key", std::make_shared<DataTypeString>()},
        {"sorting_key", std::make_shared<DataTypeString>()},
        {"primary_key", std::make_shared<DataTypeString>()},
        {"sampling_key", std::make_shared<DataTypeString>()},
        {"unique_key", std::make_shared<DataTypeString>()},
        {"storage_policy", std::make_shared<DataTypeString>()},
        {"comment", std::make_shared<DataTypeString>()},
    }));
    setInMemoryMetadata(storage_metadata);
}


class CloudTablesBlockSource : public SourceWithProgress
{
    using ResourcesType = std::vector<std::pair<UInt64, CnchWorkerResourcePtr>>;
    using TablesMap = CnchWorkerResource::TablesMap;

public:
    CloudTablesBlockSource(
        std::vector<UInt8> columns_mask_,
        Block header,
        UInt64 max_block_size_,
        ContextPtr context_)
        : SourceWithProgress(std::move(header))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , context(Context::createCopy(context_))
        , worker_resources(context->getAllWorkerResources())
        , resource_iter(worker_resources.begin())
    {
    }

    String getName() const override { return "CloudTables"; }

protected:
    Chunk generate() override
    {
        if (done || resource_iter == worker_resources.end())
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

        // const auto access = context->getAccess();
        // const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

        size_t rows_count = 0;
        while (rows_count < max_block_size)
        {
            if (tables_iter == curr_tables.cend())
            {
                ++resource_iter;
                if (resource_iter == worker_resources.end())
                {
                    done = true;
                    break;
                }
                curr_tables = resource_iter->second->getTables();
                tables_iter = curr_tables.cbegin();
            }

            for (; rows_count < max_block_size && tables_iter != curr_tables.cend(); ++tables_iter)
            {
                const auto & database_name = tables_iter->first.first;
                const auto & table_name = tables_iter->first.second;
                const auto & table = tables_iter->second;

                ++rows_count;

                size_t src_index = 0;
                size_t res_index = 0;

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database_name);

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(table_name);

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(table->getStorageUUID());

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(resource_iter->first); /// txn_id

                if (columns_mask[src_index++])
                {
                    assert(table != nullptr);
                    res_columns[res_index++]->insert(table->getName());  /// engine
                }

                if (columns_mask[src_index++])
                {
                    assert(table != nullptr);
                    Array table_paths_array;
                    auto paths = table->getDataPaths();
                    table_paths_array.reserve(paths.size());
                    for (const String & path : paths)
                        table_paths_array.push_back(path);
                    res_columns[res_index++]->insert(table_paths_array); /// data_paths
                }

                if (columns_mask[src_index++])
                {
                    res_columns[res_index++]->insert(resource_iter->second->getCreateTime());
                }

                StorageMetadataPtr metadata_snapshot = table->getInMemoryMetadataPtr();

                ASTPtr expression_ptr;
                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getPartitionKeyAST()))
                        res_columns[res_index++]->insert(queryToString(expression_ptr));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getSortingKey().expression_list_ast))
                        res_columns[res_index++]->insert(queryToString(expression_ptr));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getPrimaryKey().expression_list_ast))
                        res_columns[res_index++]->insert(queryToString(expression_ptr));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getSamplingKeyAST()))
                        res_columns[res_index++]->insert(queryToString(expression_ptr));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getUniqueKeyAST()))
                        res_columns[res_index++]->insert(queryToString(expression_ptr));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    auto policy = table ? table->getStoragePolicy(IStorage::StorageLocation::MAIN) : nullptr;
                    if (policy)
                        res_columns[res_index++]->insert(policy->getName());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot)
                        res_columns[res_index++]->insert(metadata_snapshot->comment);
                    else
                        res_columns[res_index++]->insertDefault();
                }
            }
        }

        UInt64 num_rows = res_columns.at(0)->size();
        return Chunk(std::move(res_columns), num_rows);
    }

private:
    std::vector<UInt8> columns_mask;
    UInt64 max_block_size;
    ContextPtr context;
    ResourcesType worker_resources;
    ResourcesType::const_iterator resource_iter;

    TablesMap curr_tables;
    TablesMap::const_iterator tables_iter{curr_tables.cend()};

    bool done = false;
};


Pipe StorageSystemCloudTables::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned /*num_streams*/)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    /// Create a mask of what columns are needed in the result.
    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = metadata_snapshot->getSampleBlock();
    Block res_block;

    std::vector<UInt8> columns_mask(sample_block.columns());
    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.count(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            res_block.insert(sample_block.getByPosition(i));
        }
    }

    return Pipe(std::make_shared<CloudTablesBlockSource>(
        std::move(columns_mask), std::move(res_block), max_block_size, context));
}

}
