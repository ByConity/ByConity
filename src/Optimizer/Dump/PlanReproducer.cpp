#include <Optimizer/Dump/PlanReproducer.h>

#include <Common/SettingsChanges.h>
#include <Core/QualifiedTableName.h>
#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Optimizer/Dump/DumpUtils.h>
#include <Optimizer/Dump/ReproduceUtils.h>
#include <Optimizer/Dump/StatsLoader.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Poco/JSON/Object.h>
#include <Poco/Logger.h>

#include <string>
#include <optional>
#include <unordered_map>

using namespace DB::Statistics;
using namespace DB::DumpUtils;

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_NOT_FOUND;
}

namespace
{
    std::optional<std::string> tryGetQueryInfo(Poco::JSON::Object::Ptr query_json, DumpUtils::QueryInfo query_info)
    {
        std::string query_info_string = toString(query_info);
        if (query_json && query_json->has(query_info_string))
            return query_json->getValue<std::string>(query_info_string);
        return std::nullopt;
    }

    SettingsChanges getSettingsChangesFromJson(Poco::JSON::Object::Ptr query_json)
    {
        SettingsChanges res;
        std::string settings_tag = toString(DumpUtils::QueryInfo::settings);
        if (!query_json || !query_json->has(settings_tag))
            return res;
        Poco::JSON::Object::Ptr settings_json = query_json->getObject(settings_tag);
        if (!settings_json)
            return res;
        for (const auto & [setting_name, setting_value] : *settings_json)
            res.emplace_back(setting_name, setting_value.toString());
        return res;
    }
}

PlanReproducer::Query PlanReproducer::getQuery(const std::string & query_id)
{
    if (!queries || !queries->has(query_id))
        throw Exception("query " + query_id + " is not found in source " + reproduce_path, ErrorCodes::LOGICAL_ERROR);
    Poco::JSON::Object::Ptr query_json = queries->getObject(query_id);
    if (!query_json)
        throw Exception("invalid query json", ErrorCodes::LOGICAL_ERROR);
    return Query{query_id,
                 tryGetQueryInfo(query_json, QueryInfo::query).value(),
                 tryGetQueryInfo(query_json, QueryInfo::current_database).value(),
                 getSettingsChangesFromJson(query_json),
                 tryGetQueryInfo(query_json, QueryInfo::memory_catalog_worker_size),
                 tryGetQueryInfo(query_json, QueryInfo::explain)};
}

void PlanReproducer::loadStats(ContextPtr catalog_adaptor_context, const std::unordered_set<QualifiedTableName> & tables_to_load) {
    StatsLoader loader = StatsLoader(reproduce_path, catalog_adaptor_context, stats);
    auto tables_loaded = loader.loadStats(false, tables_to_load);
    for (const auto & table : tables_loaded)
    {
        if (auto it = table_status.find(table); it != table_status.end())
        {
            switch (it->second) {
                case ReproduceUtils::DDLStatus::created:
                case ReproduceUtils::DDLStatus::created_and_loaded_stats:
                    table_status[table] = ReproduceUtils::DDLStatus::created_and_loaded_stats;
                    break;
                case ReproduceUtils::DDLStatus::reused:
                case ReproduceUtils::DDLStatus::reused_and_loaded_stats:
                    table_status[table] = ReproduceUtils::DDLStatus::reused_and_loaded_stats;
                    break;
                default:
                    table_status[table] = ReproduceUtils::DDLStatus::unknown;
            }
        } else {
            table_status.emplace(table, ReproduceUtils::DDLStatus::unknown);
        }
    }
}

void PlanReproducer::createTables(bool load_stats)
{
    if (ddls)
        for (const auto & [name, create_json] : *ddls)
            createTable(create_json.toString());

    if (views)
        for (const auto & [name, create_json] : *views)
            createTable(create_json.toString());

    if (load_stats)
    {
        std::unordered_set<QualifiedTableName> tables_to_load;
        for (const auto & [table_name, status] : table_status) {
            if (status == ReproduceUtils::DDLStatus::created)
                tables_to_load.emplace(table_name);
        }
        ContextMutablePtr stats_context = Context::createCopy(latest_context);
        loadStats(stats_context, tables_to_load);
        stats_context->applySettingsChanges(SettingsChanges{{"enable_memory_catalog", true}});
        loadStats(stats_context, tables_to_load); // reload stats otherwise memory catalog cannot find stats
    }
}

void PlanReproducer::createTable(const std::string & ddl)
{
    ContextMutablePtr query_context = makeQueryContext({SettingChange("dialect_type", "CLICKHOUSE")}); // avoid ansi nullable issue
    ASTPtr ast = ReproduceUtils::parse(ddl, query_context);
    if (!ast)
    {
        LOG_WARNING(log, "ddl is not valid, ignored: {}", ddl);
        return;
    }
    auto * create = ast->as<ASTCreateQuery>();
    if (!create)
    {
        LOG_WARNING(log, "ddl is a not valid create, ignored: {}", ddl);
        return;
    }

    std::string database_name = create->database;
    if (database_name.empty())
        database_name = latest_context->getCurrentDatabase();

    std::string table_name = create->table;

    QualifiedTableName qualified_table{database_name, table_name};

    if (table_status.contains(qualified_table))
    {
        LOG_DEBUG(log, "table {}.{} has been processed, current status is {}",
                  database_name, table_name,
                  ReproduceUtils::toString(table_status.at(qualified_table)));
        return;
    }

    // check for view inner table: create it after view to avoid duplicates
    if (table_name.starts_with(".inner."))
    {
        String mv_name = database_name + table_name.substr(6);
        if (ddls->has(mv_name))
            createTable(ddls->get(mv_name).toString());
    }

    createDatabase(database_name);

    if (DatabaseCatalog::instance().getDatabase(database_name, latest_context)->isTableExist(table_name, latest_context))
    {
        LOG_DEBUG(log, "using existing table {}.{}", database_name, table_name);
        table_status[qualified_table] = ReproduceUtils::DDLStatus::reused;
        return;
    }

    create->uuid = UUIDHelpers::Nil;
    if (cluster)
    {
        create->cluster = cluster.value();
        // update cluster specified in distributed table
        if (create->storage && create->storage->engine
            && create->storage->engine->as<ASTFunction>() && create->storage->engine->as<ASTFunction>()->name == "Distributed")
        {
            create->storage->engine->arguments->children[0] = std::make_shared<ASTLiteral>(cluster.value());
        }
    }

    ReproduceUtils::executeDDL(ast, query_context);
    updateTransaction();
    LOG_DEBUG(log, "created table {}.{}", database_name, table_name);
    table_status.emplace(std::move(qualified_table), ReproduceUtils::DDLStatus::created);
}

void PlanReproducer::createDatabase(const std::string & database_name)
{
    if (database_status.contains(database_name))
    {
        LOG_DEBUG(log, "database {} has been processed, current status is {}",
                  database_name, ReproduceUtils::toString(database_status.at(database_name)));
        return;
    }

    if (DatabaseCatalog::instance().isDatabaseExist(database_name, latest_context))
    {
        LOG_DEBUG(log, "using existing database {}", database_name);
        database_status[database_name] = ReproduceUtils::DDLStatus::reused;
        return;
    }

    auto create = std::make_shared<ASTCreateQuery>();
    create->uuid = UUIDHelpers::Nil;
    create->database = database_name;
    if (cluster)
        create->cluster = cluster.value();

    ContextMutablePtr query_context = makeQueryContext();
    ReproduceUtils::executeDDL(create, query_context);
    updateTransaction();
    LOG_DEBUG(log, "created database {}", database_name);
    database_status.emplace(database_name, ReproduceUtils::DDLStatus::created);
}

ContextMutablePtr PlanReproducer::makeQueryContext(
    const SettingsChanges & settings_changes,
    const std::optional<std::string> & database_name,
    const std::optional<std::string> & memory_catalog_worker_size)
{
    ContextMutablePtr query_context = Context::createCopy(latest_context);
    if (!settings_changes.empty())
    {
        query_context->applySettingsChanges(settings_changes); // note: use the vector version
    }

    if (database_name.has_value())
    {
        if (!DatabaseCatalog::instance().isDatabaseExist(database_name.value(), latest_context))
            createDatabase(database_name.value());
        query_context->setCurrentDatabase(database_name.value());
    }

    if (memory_catalog_worker_size.has_value())
    {
        SettingsChanges memory_catalog = {{"enable_memory_catalog", true}, {"memory_catalog_worker_size", memory_catalog_worker_size.value()}};
        query_context->applySettingsChanges(memory_catalog); // note: use the vector version
    }
    query_context->createPlanNodeIdAllocator();
    query_context->createSymbolAllocator();
    query_context->createOptimizerMetrics();
    query_context->makeQueryContext();

    return query_context;
}

void PlanReproducer::dropDatabase(const std::string & database_name)
{
    if (!DatabaseCatalog::instance().isDatabaseExist(database_name, latest_context))
    {
        LOG_DEBUG(log, "database {} is already dropped", database_name);
        return;
    }

    if (!database_status.contains(database_name) || database_status.at(database_name) != ReproduceUtils::DDLStatus::created)
    {
        LOG_DEBUG(log, "cannot drop database {}", database_name);
        return;
    }

    auto drop = std::make_shared<ASTDropQuery>();
    drop->database = database_name;
    if (cluster)
        drop->cluster = cluster.value();

    ContextMutablePtr query_context = makeQueryContext();
    ReproduceUtils::executeDDL(drop, query_context);
    updateTransaction();
    LOG_DEBUG(log, "dropped database {}", database_name);
    database_status[database_name] = ReproduceUtils::DDLStatus::dropped;
}

void PlanReproducer::dropTable(const QualifiedTableName & table)
{
    if (!DatabaseCatalog::instance().isDatabaseExist(table.database, latest_context))
    {
        LOG_DEBUG(log, "database {} is already dropped", table.database);
        return;
    }

    if (!DatabaseCatalog::instance().getDatabase(table.database, latest_context)->isTableExist(table.table, latest_context))
    {
        LOG_DEBUG(log, "table {}.{} is already dropped", table.database, table.table);
        return;
    }

    if (!table_status.contains(table) ||
        (table_status.at(table) != ReproduceUtils::DDLStatus::created && table_status.at(table) != ReproduceUtils::DDLStatus::created_and_loaded_stats))
    {
        LOG_DEBUG(log, "cannot drop table {}.{}", table.database, table.table);
        return;
    }

    auto drop = std::make_shared<ASTDropQuery>();
    drop->database = table.database;
    drop->table = table.table;
    if (cluster)
        drop->cluster = cluster.value();

    ContextMutablePtr query_context = makeQueryContext();
    ReproduceUtils::executeDDL(drop, query_context);
    updateTransaction();
    LOG_DEBUG(log, "dropped table {}.{}", table.database, table.table);
    table_status[table] = ReproduceUtils::DDLStatus::dropped;
}

void PlanReproducer::updateTransaction()
{
    if (!latest_context->getCurrentTransaction())
        return;
    auto & txn_coordinator = latest_context->getCnchTransactionCoordinator();
    TransactionCnchPtr txn = txn_coordinator.createTransaction(CreateTransactionOption().setContext(latest_context));
    latest_context->setCurrentTransaction(txn, true);
}

}
