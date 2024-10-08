#include <Storages/System/StorageSystemCnchPartsColumns.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Catalog/Catalog.h>
#include <CloudServices/CnchPartsHelper.h>
#include <Interpreters/Context.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <DataStreams/materializeBlock.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}   

NamesAndTypesList StorageSystemCnchPartsColumns::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"partition", std::make_shared<DataTypeString>()},
        {"column", std::make_shared<DataTypeString>()},
        {"type", std::make_shared<DataTypeString>()},
        {"column_data_bytes_on_disk", std::make_shared<DataTypeUInt64>()},
        {"column_data_compressed_bytes", std::make_shared<DataTypeUInt64>()},
        {"column_data_uncompressed_bytes", std::make_shared<DataTypeUInt64>()},
        {"column_skip_indice_bytes_on_disk", std::make_shared<DataTypeUInt64>()},
        {"column_skip_indice_compressed_bytes", std::make_shared<DataTypeUInt64>()},
        {"column_skip_indice_uncompressed_bytes", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemCnchPartsColumns::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    LoggerPtr log = getLogger(getName());

    ASTPtr where_expression = query_info.query->as<ASTSelectQuery>()->where();

    const std::vector<std::map<String,Field>> value_by_column_names = DB::collectWhereORClausePredicate(where_expression, context);

    bool enable_filter_by_partition = false;
    String selected_database;
    String selected_table;
    String selected_partition_id;

    if (value_by_column_names.size() == 1)
    {
        const auto & value_by_column_name = value_by_column_names.at(0);
        auto db_it = value_by_column_name.find("database");
        auto table_it = value_by_column_name.find("table");
        auto partition_it = value_by_column_name.find("partition_id");
        if ((db_it != value_by_column_name.end()) &&
            (table_it != value_by_column_name.end())
            )
        {
            selected_database = db_it->second.safeGet<String>();
            selected_table = table_it->second.safeGet<String>();
            LOG_TRACE(log, "filtering by db and table with db name {} and table name {}", selected_database, selected_table);
        }

        if (partition_it != value_by_column_name.end())
        {
            selected_partition_id = partition_it->second.safeGet<String>();
            enable_filter_by_partition = true;

            LOG_TRACE(log, "filtering from catalog by partition with partition name {}", selected_partition_id);
        }
    }

    if (selected_database.empty() || selected_table.empty())
        throw Exception(
            "Please check if the query has required columns and condition : 'database' AND 'table' .Eg. select * from "
            "cnch_parts_columns WHERE database = 'some_name' AND table = 'another_name'.... ", ErrorCodes::INCORRECT_QUERY);

    //worker side logic, fill data with preallocated parts.
    if (context->getServerType() == ServerType::cnch_worker)
    {
        auto storage =  DatabaseCatalog::instance().tryGetTable(StorageID{selected_database, selected_table}, context);
        if (!storage)
            return;

        auto * cloud = dynamic_cast<StorageCloudMergeTree *>(storage.get());
        if (!cloud)
            throw Exception("Wrong storage type for parts columns request", ErrorCodes::BAD_ARGUMENTS);

        auto columns = storage->getInMemoryMetadataPtr()->getColumns().getAllPhysical();
        cloud->prepareDataPartsForRead();
        auto parts = cloud->getDataPartsVector();

        LOG_DEBUG(log, "Target CloudMergeTree got {} parts" , parts.size());
        const FormatSettings format_settings;

        for (const auto & part : parts)
        {
            String partition;
            {
                WriteBufferFromOwnString out;
                part->partition.serializeText(*cloud, out, format_settings);
                partition = out.str();
            }

            auto skipindices_size = part->getColumnsSkipIndicesSize();

            for (auto & column : columns)
            {
                int index = 0;
                res_columns[index++]->insert(selected_database);
                res_columns[index++]->insert(selected_table);
                res_columns[index++]->insert(part->name);
                res_columns[index++]->insert(partition);
                res_columns[index++]->insert(column.name);
                res_columns[index++]->insert(column.type->getName());

                ColumnSize data_column_size = part->getColumnSize(column.name, *column.type);
                res_columns[index++]->insert(data_column_size.data_compressed + data_column_size.marks);
                res_columns[index++]->insert(data_column_size.data_compressed);
                res_columns[index++]->insert(data_column_size.data_uncompressed);

                auto skipindices_size_iter = skipindices_size.find(column.name);
                if (skipindices_size_iter != skipindices_size.end())
                {
                    res_columns[index++]->insert(skipindices_size_iter->second.data_compressed + skipindices_size_iter->second.marks);
                    res_columns[index++]->insert(skipindices_size_iter->second.data_compressed);
                    res_columns[index++]->insert(skipindices_size_iter->second.data_uncompressed);
                }
                else
                {
                    res_columns[index++]->insert(0);
                    res_columns[index++]->insert(0);
                    res_columns[index++]->insert(0);
                }
            }
        }

        return;
    }

    // In server side, rewrite select query and send it to worker.
    auto catalog = context->getCnchCatalog();
    
    TransactionCnchPtr cnch_txn = context->getCurrentTransaction();
    TxnTimestamp current_ts = cnch_txn ? cnch_txn->getStartTime() : TxnTimestamp{context->getTimestamp()};

    auto table = catalog->tryGetTable(*context, selected_database, selected_table, current_ts);
    if (!table)
        return;
    
    auto * data = dynamic_cast<StorageCnchMergeTree *>(table.get());

    if (!data)
        throw Exception("Table system.cnch_parts_columns only support CnchMergeTree engine", ErrorCodes::NOT_IMPLEMENTED);

    auto worker_group = getWorkerGroupForTable(*data, context);
    context->setCurrentWorkerGroup(worker_group);

    auto all_parts = enable_filter_by_partition
        ? catalog->getServerDataPartsInPartitions(table, {selected_partition_id}, current_ts, nullptr)
        : catalog->getAllServerDataParts(table, current_ts, nullptr);


    auto visible_parts = CnchPartsHelper::calcVisibleParts(all_parts, false);
    if (visible_parts.empty())
        return;

    LOG_DEBUG(log, "visible parts size :  {}" , visible_parts.size());
    data->allocateParts(context, visible_parts);

    String inner_query = "select * from system.cnch_parts_columns where database = '" + data->getDatabaseName() + "' and table = '" + data->getCloudTableName(context) + "'";
    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, inner_query.data(), inner_query.data() + inner_query.size(), "", 0, 0);

    Block header = materializeBlock(InterpreterSelectQuery(ast, context, QueryProcessingStage::Complete).getSampleBlock());

    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    auto storage_snapshot = table->getStorageSnapshot(metadata_snapshot, context);
    ClusterProxy::SelectStreamFactory stream_factory = ClusterProxy::SelectStreamFactory(
        header,
        {},
        storage_snapshot, 
        QueryProcessingStage::Complete, 
        StorageID{"system", "cnch_parts_columns"}, 
        Scalars{}, 
        false, 
        {});

    QueryPlan query_plan;
    ClusterProxy::executeQuery(query_plan, stream_factory, log, ast, context, worker_group);

    if (!query_plan.isInitialized())
        throw Exception("Pipeline is not initialized", ErrorCodes::LOGICAL_ERROR);

    auto pipeline = std::move(*query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context)));

    BlockInputStreamPtr input_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

    auto fill_const_column = [&](MutableColumnPtr & col, const String & val, size_t rows)
    {
        while (rows--)
            col->insert(val);
    };

    while (auto block = input_stream->read())
    {
        size_t rows = block.rows();
        fill_const_column(res_columns[0], selected_database, rows);
        fill_const_column(res_columns[1], selected_table, rows);
        for (size_t i = 2; i<res_columns.size(); i++)
        {
            res_columns[i]->insertRangeFrom(static_cast<const IColumn &>(*(block.getByPosition(i).column)), 0, rows);
        }
    }
}


}
