#include <Optimizer/Dump/DDLDumper.h>

#include <Optimizer/Dump/ProtoEnumUtils.h>

#include <Core/Names.h>
#include <Core/QualifiedTableName.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Poco/JSON/Parser.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/PlanVisitor.h>
#include <Statistics/StatisticsBase.h>
#include <Statistics/StatisticsCollector.h>

#include <Storages/IStorage_fwd.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageView.h>
#include <Storages/StorageMaterializedView.h>
#include <Optimizer/Dump/DumpUtils.h>

#include <chrono>
#include <string>
#include <optional>

using namespace DB::DumpUtils;

namespace DB
{
using namespace Statistics;

void DDLDumper::addTable(const std::string & database_name, const std::string & table_name, ContextPtr context)
{
    addTable(QualifiedTableName{database_name, table_name}, context);
}


void DDLDumper::addTable(const QualifiedTableName & qualified_table, ContextPtr context)
{
    if (qualified_table.database.empty() || qualified_table.database == DatabaseCatalog::SYSTEM_DATABASE || visited_tables.contains(qualified_table))
        return;
    
    visited_tables.insert(qualified_table);
    const auto & database = qualified_table.database;
    const auto & table = qualified_table.table;

    if (!DatabaseCatalog::instance().isDatabaseExist(database, context)) {
        LOG_WARNING(log, "database {} not found", database);
        return;
    }
    auto database_ptr = DatabaseCatalog::instance().getDatabase(database, context);
    StoragePtr storage = database_ptr->tryGetTable(table, context);
    if (!storage) {
        LOG_WARNING(log, "table {} not found in database {}", table, database);
        return;
    }

    auto create_query = dynamic_pointer_cast<ASTCreateQuery>(database_ptr->getCreateTableQuery(table, context));
    if (!create_query) {
        LOG_WARNING(log, "invalid create query for {}.{}", database, table);
        return;
    }
    // remove the UUID
    create_query->uuid = UUIDHelpers::Nil;

    if (auto view = dynamic_pointer_cast<const StorageView>(storage))
    {
        // for views, we add table from its inner query, and we separate its create-table statement
        ASTPtr inner_query = view->getInMemoryMetadataPtr()->getSelectQuery().inner_query;
        addTableFromSelectQuery(inner_query, context);
        view_ddls.emplace(qualified_table, serializeAST(*create_query));
    }
    else if (auto materialized_view = dynamic_pointer_cast<const StorageMaterializedView>(storage))
    {
        // for mv, we need to redirect the create-statement to its inner table if not already done
        ASTPtr inner_query = materialized_view->getInnerQuery();
        addTableFromSelectQuery(inner_query, context);
        auto target_id = materialized_view->getTargetTableId();
        addTable(target_id.getQualifiedName(), context);

        if (!create_query->is_materialized_view)
        {
            LOG_WARNING(log, "invalid create query for {}.{}", database, table);
            return;
        }
        if (!create_query->to_table_id)
        {
            create_query->to_table_id = target_id;
            create_query->to_inner_uuid = UUIDHelpers::Nil;
            create_query->storage = nullptr;
            create_query->columns_list = nullptr;
        }
        view_ddls.emplace(qualified_table, serializeAST(*create_query));
    }
    else
    {
        table_ddls.emplace(qualified_table, serializeAST(*create_query));
        if (settings.stats)
        {
            auto json = getTableStats(database, table, context);
            if (json)
                stats->set(qualified_table.getFullName(), json);
        }
        if (auto storage_distributed = dynamic_pointer_cast<const StorageDistributed>(storage))
        {
            // distributed table
            if (settings.shard_count)
            {
                auto shard_count = storage_distributed->getShardCount();
                shard_counts.emplace(qualified_table, shard_count);
            }
            // add local table ddl
            addTable(storage_distributed->getRemoteDatabaseName(), storage_distributed->getRemoteTableName(), context);
        }
    }
}

std::optional<size_t> DDLDumper::addTableFromSelectQuery(ASTPtr query_ptr, ContextPtr context, const NameSet & with_tables_context)
{
    if (const auto * select_query = query_ptr->as<const ASTSelectQuery>())
        return addTableFromAST(query_ptr, context, with_tables_context);

    if (const auto * subquery = query_ptr->as<const ASTSubquery>())
        return addTableFromSelectQuery(subquery->children.at(0), context, with_tables_context);

    if (const auto * select_with_union_query = query_ptr->as<const ASTSelectWithUnionQuery>())
    {
        std::optional<size_t> shard_count = std::nullopt;
        for (const auto & select : select_with_union_query->list_of_selects->children)
        {
            auto select_shard_count = addTableFromSelectQuery(select, context, with_tables_context);
            if (!shard_count)
                shard_count = select_shard_count;
        }
        return shard_count;
    }

    throw Exception("Dump only supports select query", ErrorCodes::LOGICAL_ERROR);
}

std::optional<size_t> DDLDumper::addTableFromAST(ASTPtr ast, ContextPtr context, const NameSet & with_tables_context)
{
    std::optional<size_t> shard_count = std::nullopt;
    NameSet with_tables = with_tables_context;
    for (const auto & child : ast->children)
    {
        // collect with tables
        if (auto * with_elem = child->as<ASTWithElement>())
        {
            auto shard_count_opt = addTableFromSelectQuery(with_elem->subquery, context, with_tables);
            if (!shard_count)
                shard_count = shard_count_opt;
            with_tables.emplace(with_elem->name);
        }
        else if (const auto * table_expression = child->as<ASTTableExpression>())
        {
            // collect table expressions
            if (table_expression->subquery)
            {
                auto shard_count_opt = addTableFromSelectQuery(table_expression->subquery, context, with_tables);
                if (!shard_count)
                    shard_count = shard_count_opt;
            }
            else if (table_expression->table_function)
            {
                LOG_WARNING(log, "cannot dump a table within table function");
            }
            else if (table_expression->database_and_table_name)
            {
                auto storage_id_unresolved = StorageID(table_expression->database_and_table_name);
                auto & database_name = storage_id_unresolved.database_name;
                auto & table_name = storage_id_unresolved.table_name;
                // with-table
                if (database_name.empty() && with_tables.contains(table_name))
                    continue;

                auto storage_id = context->tryResolveStorageID(storage_id_unresolved); // attach current database if needed
                if (storage_id.database_name.empty() || storage_id.database_name == DatabaseCatalog::SYSTEM_DATABASE)
                    continue;

                addTable(storage_id.database_name, storage_id.table_name, context);
                if (!shard_count && shard_counts.contains(QualifiedTableName{storage_id.database_name, storage_id.table_name}))
                    shard_count = shard_counts.at(QualifiedTableName{storage_id.database_name, storage_id.table_name});
            }
        }
        else
        {
            auto shard_count_opt = addTableFromAST(child, context, with_tables_context);
            if (!shard_count)
                shard_count = shard_count_opt;
        }
    }
    return shard_count;
}

void DDLDumper::addTableFromDatabase(DatabasePtr database, ContextPtr context)
{
    auto start_watch = std::chrono::high_resolution_clock::now();

    for (auto it = database->getTablesIterator(context); it->isValid(); it->next())
    {
        addTable(database->getDatabaseName(), it->name(), context);
    }

    auto stop_watch = std::chrono::high_resolution_clock::now();
    LOG_DEBUG(log, "Add tables from database {} cost: {} ms", database->getDatabaseName(),
              std::chrono::duration_cast<std::chrono::milliseconds>(stop_watch - start_watch).count());
}

void DDLDumper::addTableFromDatabase(const std::string & database_name, ContextPtr context)
{
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(database_name, context);
    if (!database)
    {
        LOG_WARNING(log, "database {} does not exist", database_name);
        return;
    }
    return addTableFromDatabase(database, context);
}

void DDLDumper::addTableAll(ContextPtr context)
{
    Databases databases = DatabaseCatalog::instance().getDatabases(context);
    for (const auto & elem : databases)
    {
        if (elem.first != DatabaseCatalog::SYSTEM_DATABASE)
            addTableFromDatabase(elem.second, context);
    }
}

Poco::JSON::Object::Ptr DDLDumper::getJsonDumpResult()
{
    Poco::JSON::Object::Ptr tables_json(new Poco::JSON::Object);
    if (!table_ddls.empty())
    {
        Poco::JSON::Object::Ptr ddl_json(new Poco::JSON::Object);
        for (const auto & ddl : table_ddls)
            ddl_json->set(ddl.first.getFullName(), ddl.second);
        tables_json->set("ddls", ddl_json);
    }

    if (!view_ddls.empty())
    {
        Poco::JSON::Object::Ptr view_json(new Poco::JSON::Object);
        for (const auto & view : view_ddls)
            view_json->set(view.first.getFullName(), view.second);
        tables_json->set("views", view_json);
    }

    if (settings.stats && stats && stats->size() != 0)
        tables_json->set("stats", stats);
    
    if (settings.shard_count && !shard_counts.empty())
    {
        Poco::JSON::Object::Ptr shard_json(new Poco::JSON::Object);
        for (const auto & shard : shard_counts)
            shard_json->set(shard.first.getFullName(), std::to_string(shard.second));
        tables_json->set("shard_count", shard_json);
    }
    return tables_json;
}

void DDLDumper::dumpStats(const std::optional<std::string> & absolute_path)
{
    if (absolute_path.has_value())
        DumpUtils::writeJsonToAbsolutePath(*stats, absolute_path.value());
}

Poco::JSON::Object::Ptr DDLDumper::getTableStats(const std::string & database_name, const std::string & table_name, ContextPtr context)
{
    Poco::JSON::Object::Ptr json(new Poco::JSON::Object);
    auto catalog = Statistics::createCatalogAdaptor(context);
    auto table_id = catalog->getTableIdByName(database_name, table_name);
    if (!table_id)
        return json;

    try
    {
        Statistics::StatisticsCollector collector(context, catalog, table_id.value(), CollectorSettings{});
        collector.readAllFromCatalog();
        auto table_collection = collector.getTableStats().writeToCollection();
        if (table_collection.empty())
            return json;

        for (auto & [table_tag, table_stats] : table_collection)
        {
            if (table_tag != Statistics::StatisticsTag::Invalid)
                json->set(ProtoEnumUtils::statisticsTagToString(table_tag),
                          Poco::JSON::Parser().parse(table_stats->serializeToJson())); // todo: may not need to parse
        }

        Poco::JSON::Object::Ptr columns(new Poco::JSON::Object);
        for (const auto & [column_name, column_collection_map] : collector.getColumnsStats())
        {
            auto column_collection = column_collection_map.writeToCollection();
            if (column_collection.empty())
                continue;
            // column_json = { "column_tag" : tag_stats_in_json}
            Poco::JSON::Object::Ptr column_json(new Poco::JSON::Object);
            for (auto & [column_tag, column_stats] : column_collection)
            {
                if (column_tag != Statistics::StatisticsTag::Invalid)
                    column_json->set(
                        ProtoEnumUtils::statisticsTagToString(column_tag),
                        Poco::JSON::Parser().parse(column_stats->serializeToJson()));
            }

            columns->set(column_name, column_json);
        }
        json->set("Columns", columns);
    }
    catch (Exception & e)
    {
        LOG_DEBUG(log, "failed to obtain stats for table {}.{}, reason: {}", database_name, table_name, e.message());
    }

    return json;
}

}
