#include <Backups/BackupSnapshotMaker.h>
#include <Backups/BackupsWorker.h>
#include <Catalog/Catalog.h>
#include <Interpreters/Context.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int INVALID_CNCH_SNAPSHOT;
    extern const int BACKUP_JOB_CREATE_FAILED;
}

namespace BackupSnapshotMaker
{
    BackupSnapshot createSnapshotForTable(DatabasePtr database, StoragePtr table, const String & backup_id, const ContextPtr & context)
    {
        if (database->getEngineName() != "Cnch")
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot create snapshot under database of type '{}'", database->getEngineName());
        UUID db_uuid = database->getUUID();
        if (db_uuid == UUIDHelpers::Nil)
            /// TODO: add UUID for db
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Require UUID for DB");

        UUID table_uuid = table->getStorageUUID();
        if (table_uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Require UUID for TABLE");

        String table_uuid_str = UUIDHelpers::UUIDToString(table_uuid);

        String snapshot_name = backup_id + "/" + table_uuid_str;

        auto ts = context->getCurrentTransactionID();
        context->getCnchCatalog()->createSnapshot(db_uuid, snapshot_name, ts, MAX_SNAPSHOT_TTL, table_uuid);

        return {db_uuid, snapshot_name};
    }

    BackupSnapshots makeSnapshots(const ASTBackupQuery & backup_query, const String & backup_id, const ContextPtr & context)
    {
        BackupSnapshots snapshots;

        try
        {
            for (const auto & element : backup_query.elements)
            {
                switch (element.type)
                {
                    case ASTBackupQuery::ElementType::TABLE: {
                        DatabasePtr database = DatabaseCatalog::instance().getDatabase(element.database_name, context);
                        if (database == nullptr)
                            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "database {} does not exist.", element.database_name);
                        StoragePtr table = database->tryGetTable(element.table_name, context);
                        if (table == nullptr)
                            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist.", element.table_name);

                        std::shared_ptr<StorageCnchMergeTree> cnch_table = std::dynamic_pointer_cast<StorageCnchMergeTree>(table);
                        if (cnch_table)
                            snapshots.emplace_back(createSnapshotForTable(database, table, backup_id, context));
                        break;
                    }
                    case ASTBackupQuery::ElementType::DATABASE: {
                        DatabasePtr database = DatabaseCatalog::instance().getDatabase(element.database_name, context);
                        if (database == nullptr)
                            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "database {} does not exist.", element.database_name);
                        std::unordered_set<String> except_table_names;
                        if (!element.except_tables.empty())
                        {
                            for (const auto & except_table : element.except_tables)
                            {
                                if (except_table.first == database->getDatabaseName())
                                    except_table_names.emplace(except_table.second);
                            }
                        }
                        auto filter_by_table_name
                            = [&except_table_names](const String & table_name) { return !except_table_names.contains(table_name); };
                        for (auto it = database->getTablesIterator(context, filter_by_table_name); it->isValid(); it->next())
                        {
                            StoragePtr table = it->table();
                            if (table == nullptr)
                                continue; /// Probably the table has been just dropped.
                            std::shared_ptr<StorageCnchMergeTree> cnch_table = std::dynamic_pointer_cast<StorageCnchMergeTree>(table);
                            if (cnch_table)
                                snapshots.emplace_back(createSnapshotForTable(database, table, backup_id, context));
                        }
                        break;
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("BackupSnapshotMaker"), "Backup task create failed because: ");
            Catalog::CatalogPtr catalog = context->getCnchCatalog();
            for (BackupSnapshot & snapshot : snapshots)
            {
                catalog->removeSnapshot(snapshot.first, snapshot.second);
            }
            throw;
        }

        return snapshots;
    }

    void clearSnapshots(const ASTBackupQuery & backup_query, const String & backup_id, const ContextPtr & context)
    {
        BackupSnapshots snapshots;
        for (const auto & element : backup_query.elements)
        {
            switch (element.type)
            {
                case ASTBackupQuery::ElementType::TABLE: {
                    DatabasePtr database = DatabaseCatalog::instance().getDatabase(element.database_name, context);
                    if (database == nullptr)
                        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "database {} does not exist.", element.database_name);
                    StoragePtr table = database->tryGetTable(element.table_name, context);
                    if (table == nullptr)
                        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist.", element.table_name);
                    std::shared_ptr<StorageCnchMergeTree> cnch_table = std::dynamic_pointer_cast<StorageCnchMergeTree>(table);
                    if (cnch_table)
                    {
                        String snapshot_name = backup_id + "/" + UUIDHelpers::UUIDToString(table->getStorageUUID());
                        removeSnapshot(context, {database->getUUID(), snapshot_name});
                    }

                    break;
                }
                case ASTBackupQuery::ElementType::DATABASE: {
                    DatabasePtr database = DatabaseCatalog::instance().getDatabase(element.database_name, context);
                    if (database == nullptr)
                        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "database {} does not exist.", element.database_name);
                    std::unordered_set<String> except_table_names;
                    if (!element.except_tables.empty())
                    {
                        for (const auto & except_table : element.except_tables)
                        {
                            if (except_table.first == database->getDatabaseName())
                                except_table_names.emplace(except_table.second);
                        }
                    }
                    auto filter_by_table_name
                        = [&except_table_names](const String & table_name) { return !except_table_names.contains(table_name); };
                    for (auto it = database->getTablesIterator(context, filter_by_table_name); it->isValid(); it->next())
                    {
                        StoragePtr table = it->table();
                        if (table == nullptr)
                            continue;
                        String snapshot_name = backup_id + "/" + UUIDHelpers::UUIDToString(table->getStorageUUID());
                        removeSnapshot(context, {database->getUUID(), snapshot_name});
                    }
                    break;
                }
            }
        }
    }

    void removeSnapshot(const ContextPtr & context, BackupSnapshot snapshot)
    {
        Catalog::CatalogPtr catalog = context->getCnchCatalog();

        catalog->removeSnapshot(snapshot.first, snapshot.second);
        // Check snapshot is removed successfully
        if (catalog->tryGetSnapshot(snapshot.first, snapshot.second))
        {
            throw Exception(ErrorCodes::INVALID_CNCH_SNAPSHOT, "Snapshot {} clear failed.", snapshot.second);
        }
    }
}
}
