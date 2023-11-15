#include <Databases/MySQL/DatabaseCnchMaterializedMySQL.h>

#if USE_MYSQL

#include <Catalog/Catalog.h>
#include <Databases/IDatabase.h>
#include <Databases/MySQL/DatabaseMaterializeTablesIterator.h>
#include <Databases/MySQL/MaterializedMySQLSyncThreadManager.h>
#include <DaemonManager/DaemonManagerClient.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <MergeTreeCommon/CnchStorageCommon.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>
#include <Storages/StorageMaterializeMySQL.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Transaction/Actions/DDLAlterAction.h>
#include <Transaction/Actions/DDLCreateAction.h>
#include <Transaction/Actions/DDLDropAction.h>
#include <Transaction/Actions/DDLRenameAction.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Transaction/ICnchTransaction.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int CNCH_TRANSACTION_NOT_INITIALIZED;
    extern const int SYSTEM_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
}

static void checkSettings(const MaterializeMySQLSettings & settings_local)
{
    if (!settings_local.include_tables.value.empty() && !settings_local.exclude_tables.value.empty())
        throw Exception("Can not set both settings: include_tables and exclude_tables", ErrorCodes::BAD_ARGUMENTS);
}

DatabaseCnchMaterializedMySQL::DatabaseCnchMaterializedMySQL(ContextPtr local_context,
                                                             const String & database_name_, UUID uuid_,
                                                             const MySQLDatabaseInfo & mysql_info_,
                                                             std::unique_ptr<MaterializeMySQLSettings> settings_,
                                                             const String & create_sql_)
    : DatabaseCnch(database_name_, uuid_, "DatabaseCnchMaterializedMySQL ( " + database_name_ + " )", local_context->getGlobalContext())
    , mysql_info(mysql_info_)
    , settings(std::move(settings_))
    , create_sql(create_sql_)
{
}

bool DatabaseCnchMaterializedMySQL::shouldSyncTable(const String & table_name) const
{
    if (settings->include_tables.value.empty() && settings->exclude_tables.value.empty())
        return true;
    if (!settings->include_tables.value.empty())
    {
        const auto & include_tables = settings->include_tables.value;
        for (const String & s: include_tables)
        {
            if (std::regex_match(table_name, std::regex(s)))
                return true;
        }
        return false;
    }
    else
    {
        const auto & exclude_tables = settings->exclude_tables.value;
        for (const String & s: exclude_tables)
        {
            if (std::regex_match(table_name, std::regex(s)))
                return false;
        }
        return true;
    }
}

bool DatabaseCnchMaterializedMySQL::shouldSkipDDLCmd(const String & query) const
{
    if (settings->skip_ddl_patterns.value.empty())
        return false;
    const auto & skip_ddl_patterns = settings->skip_ddl_patterns.value;
    for (const String & pattern: skip_ddl_patterns)
    {
        if (std::regex_match(query, std::regex(pattern, std::regex_constants::icase)))
            return true;
    }
    return false;
}

void DatabaseCnchMaterializedMySQL::manualResyncTable(const String & table_name, ContextPtr local_context)
{
    if (!shouldSyncTable(table_name))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Database {} shouldn't sync table {}, please check include_tables and exclude_tables setting",
            database_name,
            table_name);
    /// Try to forward RESYNC query to the target server if needs to
    if (local_context->getSettings().enable_auto_query_forwarding)
    {
        auto cnch_table_helper = CnchStorageCommonHelper(getStorageID(), getDatabaseName(), getStorageID().getTableName());
        if (cnch_table_helper.forwardQueryToServerIfNeeded(local_context, getStorageID()))
            return;
    }
    auto bg_thread = getContext()->getCnchBGThread(CnchBGThreadType::MaterializedMySQL, getStorageID());
    auto * manager = dynamic_cast<MaterializedMySQLSyncThreadManager*>(bg_thread.get());
    if (!manager)
        throw Exception("Convert to MaterializedMySQLSyncThreadManager from bg_thread failed", ErrorCodes::LOGICAL_ERROR);
    manager->manualResyncTable(table_name);
}

void DatabaseCnchMaterializedMySQL::createTable(ContextPtr local_context, const String & table_name, const StoragePtr & table, const ASTPtr & query)
{
    if (!local_context->getSettings().force_manipulate_materialized_mysql_table)
        throw Exception("Cannot create table without force_manipulate_materialized_mysql_table, engine: " + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);

    DatabaseCnch::createTable(local_context, table_name, table, query);
}

void DatabaseCnchMaterializedMySQL::dropTable(ContextPtr local_context, const String & table_name, bool no_delay)
{
    if (!local_context->getSettings().force_manipulate_materialized_mysql_table)
        throw Exception("Cannot drop table without force_manipulate_materialized_mysql_table, engine: " + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);

    DatabaseCnch::dropTable(local_context, table_name, no_delay);
}

void DatabaseCnchMaterializedMySQL::renameTable(
    ContextPtr local_context, const String & table_name, IDatabase & to_database, const String & to_table_name, bool exchange, bool dictionary)
{
    if (!local_context->getSettings().force_manipulate_materialized_mysql_table)
        throw Exception("Cannot rename table without force_manipulate_materialize_mysql_table, engine: " + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);

    if (to_database.getDatabaseName() != getDatabaseName())
        throw Exception("Cannot RENAME table to another database for " + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);

    DatabaseCnch::renameTable(local_context, table_name, to_database, to_table_name, exchange, dictionary);
}

StoragePtr DatabaseCnchMaterializedMySQL::tryGetTable(const String & name, ContextPtr local_context) const
{
    StoragePtr nested_storage = DatabaseCnch::tryGetTable(name, local_context);
    if (!nested_storage)
        return {};

    if (!MaterializeMySQLSyncThread::isMySQLSyncThread() && !local_context->getSettings().force_manipulate_materialized_mysql_table)
        return std::make_shared<StorageMaterializeMySQL>(std::move(nested_storage), this);

    return nested_storage;
}

DatabaseTablesIteratorPtr DatabaseCnchMaterializedMySQL::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread() && !local_context->getSettingsRef().force_manipulate_materialized_mysql_table)
    {
        DatabaseTablesIteratorPtr iterator = DatabaseCnch::getTablesIterator(local_context, filter_by_table_name);
        return std::make_unique<DatabaseMaterializeTablesIterator>(std::move(iterator), this);
    }

    return DatabaseCnch::getTablesIterator(local_context, filter_by_table_name);
}

ASTPtr DatabaseCnchMaterializedMySQL::getCreateDatabaseQuery() const
{
    ParserCreateQuery parser;
    auto ast = parseQuery(parser, create_sql, getContext()->getSettingsRef().max_query_size,
            getContext()->getSettingsRef().max_parser_depth);

    if (!ast)
        throw Exception("Fail to parseQuery for database", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);

    return ast;
}

void DatabaseCnchMaterializedMySQL::createEntryInCnchCatalog(ContextPtr local_context, String create_query) const
{
    auto txn = local_context->getCurrentTransaction();
    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    CreateActionParams params = {{getDatabaseName(), "fake", getUUID()}, create_query};
    params.is_database = true;
    params.engine_name = "CnchMaterializedMySQL";
    auto create_db = txn->createAction<DDLCreateAction>(std::move(params));
    txn->appendAction(std::move(create_db));
    txn->commitV1();
}

bool DatabaseCnchMaterializedMySQL::bgJobIsActive() const
{
    auto catalog = getContext()->getGlobalContext()->getCnchCatalog();
    std::optional<CnchBGThreadStatus> thread_status = catalog->getBGJobStatus(getUUID(), CnchBGThreadType::MaterializedMySQL);
    return (!thread_status) || (*thread_status == CnchBGThreadStatus::Running);
}

void DatabaseCnchMaterializedMySQL::applySettingsChanges(const SettingsChanges & settings_changes, ContextPtr query_context)
{
    /// Try to forward Alter database setting query to the target server if needs to
    if (query_context->getSettings().enable_auto_query_forwarding)
    {
        auto cnch_table_helper = CnchStorageCommonHelper(getStorageID(), getDatabaseName(), getStorageID().getTableName());
        if (cnch_table_helper.forwardQueryToServerIfNeeded(query_context, getStorageID()))
            return;
    }
    std::lock_guard lock(handler_mutex);

    auto daemon_manager = getContext()->getGlobalContext()->getDaemonManagerClient();
    bool materialized_job_is_active = bgJobIsActive();
    const String full_name = getStorageID().getNameForLogs();
    if (materialized_job_is_active)
    {
        LOG_TRACE(log, "Stop materialized job before altering database {}", full_name);
        daemon_manager->controlDaemonJob(getStorageID(), CnchBGThreadType::MaterializedMySQL, CnchBGThreadAction::Stop);
    }

    SCOPE_EXIT({
        if (materialized_job_is_active)
        {
            LOG_TRACE(log, "Restart materialized job no matter if ALTER succ for database {}", full_name);
            try
            {
                daemon_manager->controlDaemonJob(getStorageID(), CnchBGThreadType::MaterializedMySQL, CnchBGThreadAction::Start);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to restart materialized job for database " + full_name + " after ALTER");
            }
        }
    });

    /// Check settings
    MaterializeMySQLSettings settings_local = *settings;
    for (const auto & change : settings_changes)
    {
        if (!settings_local.has(change.name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine {} does not support setting `{}`", getEngineName(), change.name);

        if (change.name != "include_tables" && change.name != "exclude_tables" && change.name != "skip_ddl_patterns")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine {} can not modify setting `{}` other than include_tables/exclude_tables/skip_ddl_patterns", getEngineName(), change.name);

        settings_local.applyChange(change);
    }
    checkSettings(settings_local);
    for (const auto & change : settings_changes)
        settings->applyChange(change);

    TransactionCnchPtr txn = query_context->getCurrentTransaction();
    /// Apply alter commands to create-sql
    {
        ASTPtr ast = getCreateDatabaseQuery();
        auto * create = ast->as<ASTCreateQuery>();
        auto * settings = create->storage->settings;
        if (settings)
        {
            auto & storage_settings = settings->changes;
            for (const auto & change : settings_changes)
            {
                auto it = std::find_if(storage_settings.begin(), storage_settings.end(),
                                    [&](const auto & prev){ return prev.name == change.name; });
                if (it != storage_settings.end())
                    it->value = change.value;
                else
                    storage_settings.push_back(change);
            }
        }
        else
        {
            auto storage_settings = std::make_shared<ASTSetQuery>();
            storage_settings->is_standalone = false;
            storage_settings->changes = settings_changes;
            create->storage->set(create->storage->settings, storage_settings->clone());
        }

        AlterDatabaseActionParams params = {{getDatabaseName(), "fake", getUUID()}, queryToString(*create), false, ""};
        params.is_database = true;
        params.engine_name = "CnchMaterializedMySQL";
        auto action = std::make_shared<DDLAlterAction>(query_context, txn->getTransactionID(), params, query_context->getSettingsRef());
        txn->appendAction(std::move(action));
    }
    auto & txn_coordinator = query_context->getCnchTransactionCoordinator();
    txn_coordinator.commitV1(txn);
}

} /// namespace DB

#endif
