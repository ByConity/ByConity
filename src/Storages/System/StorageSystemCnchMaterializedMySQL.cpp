#include <Storages/System/StorageSystemCnchMaterializedMySQL.h>
#if USE_MYSQL

#include <Catalog/Catalog.h>
#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{
StorageSystemCnchMaterializedMySQL::StorageSystemCnchMaterializedMySQL(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        { "mysql_info",                 std::make_shared<DataTypeString>() },
        { "mysql_database",             std::make_shared<DataTypeString>()},
        { "database",                   std::make_shared<DataTypeString>() },
        { "uuid",                       std::make_shared<DataTypeString>() },
        { "sync_type",                  std::make_shared<DataTypeString>() },
        { "include_tables",             std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "exclude_tables",             std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "resync_tables",              std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "total_position",             std::make_shared<DataTypeString>() },

        { "sync_threads_number",        std::make_shared<DataTypeUInt32>() },
        { "sync_tables_list",           std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "sync_thread_names",          std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "sync_thread_clients",        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "sync_thread_binlog",         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "broken_sync_threads",        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },

        { "last_exception",             std::make_shared<DataTypeString>() },
        { "sync_failed_tables",         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "skipped_unsupported_tables", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
    }));
    setInMemoryMetadata(storage_metadata);
}

/// Create local query for `system.materialized_mysql`
static ASTPtr getSelectQuery(const SelectQueryInfo & query_info)
{
    if (!query_info.syntax_analyzer_result)
        throw Exception("No syntax_analyzer_result found in SelectQueryInfo", ErrorCodes::LOGICAL_ERROR);
    if (!query_info.query)
        throw Exception("No query found in SelectQueryInfo", ErrorCodes::LOGICAL_ERROR);

    auto required_columns = query_info.syntax_analyzer_result->requiredSourceColumns();

    auto select_query = std::make_shared<ASTSelectQuery>();
    auto select_expression_list = std::make_shared<ASTExpressionList>();
    for (const auto & column_name: required_columns)
        select_expression_list->children.push_back(std::make_shared<ASTIdentifier>(column_name));

    auto tables_list = std::make_shared<ASTTablesInSelectQuery>();
    auto element = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expr = std::make_shared<ASTTableExpression>();
    auto table_ast = std::make_shared<ASTTableIdentifier>("system", "materialized_mysql");
    table_expr->database_and_table_name = table_ast;
    table_expr->children.push_back(table_ast);
    element->table_expression = table_expr;
    element->children.push_back(table_expr);
    tables_list->children.push_back(element);

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_list));

    if (auto where_expression = query_info.query->as<ASTSelectQuery &>().where())
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));

    return select_query;
}

Pipe StorageSystemCnchMaterializedMySQL::read(
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    Catalog::CatalogPtr cnch_catalog = context->getCnchCatalog();
    if (context->getServerType() != ServerType::cnch_server || !cnch_catalog)
        throw Exception("Table system.cnch_materialized_mysql is only supported on cnch_server side", ErrorCodes::NOT_IMPLEMENTED);

    auto select_query = getSelectQuery(query_info);
    Block header = materializeBlock(InterpreterSelectQuery(select_query, context, QueryProcessingStage::Complete).getSampleBlock());
    ClusterProxy::SelectStreamFactory select_stream_factory = ClusterProxy::SelectStreamFactory(
                                            header, {}, {}, QueryProcessingStage::Complete, StorageID("system", "materialized_mysql"), {}, false, {});

    /// Set `query_info.cluster` to forward query to all instances of `server cluster`
    query_info.cluster = context->mockCnchServersCluster();

    QueryPlan query_plan;
    LoggerPtr log = getLogger("SystemCnchMaterializedMySQL");
    ClusterProxy::executeQuery(query_plan, select_stream_factory, log, select_query, context, query_info, nullptr, {}, nullptr);

    return query_plan.convertToPipe(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
}

} /// namespace DB

#endif
