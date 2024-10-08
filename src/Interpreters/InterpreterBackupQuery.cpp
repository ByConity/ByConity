#include <Interpreters/InterpreterBackupQuery.h>

#include <Access/AccessFlags.h>
#include <Access/AccessType.h>
#include <Backups/BackupDiskInfo.h>
#include <Catalog/Catalog.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeEnum.h>
#include <Databases/DatabaseCnch.h>
#include <Parsers/ASTBackupQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageCnchMergeTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DISK;
    extern const int BAD_ARGUMENTS;
    extern const int BACKUP_JOB_UNSUPPORTED;
    extern const int BACKUP_JOB_CREATE_FAILED;
    extern const int BACKUP_ALREADY_EXISTS;
    extern const int RESTORE_TABLE_ALREADY_EXIST;
}

namespace
{
    Block getResultRow(const BackupResult & backup_result)
    {
        auto column_id = ColumnString::create();
        auto column_status = ColumnInt8::create();
        auto column_info = ColumnString::create();

        column_id->insert(backup_result.backup_id);
        column_status->insert(static_cast<Int8>(backup_result.status));
        column_info->insert(backup_result.info);

        Block res_columns;
        res_columns.insert(0, {std::move(column_id), std::make_shared<DataTypeString>(), "id"});
        res_columns.insert(1, {std::move(column_status), std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues()), "status"});
        res_columns.insert(2, {std::move(column_info), std::make_shared<DataTypeString>(), "info"});

        return res_columns;
    }
}

void InterpreterBackupQuery::check()
{
    // Check table and database all exists
    ASTBackupQuery & backup_query = query_ptr->as<ASTBackupQuery &>();
    backup_query.setCurrentDatabase(context->getCurrentDatabase());

    std::vector<StoragePtr> backup_tables;
    for (const auto & element : backup_query.elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::ElementType::TABLE: {
                if (backup_query.kind == ASTBackupQuery::BACKUP)
                {
                    String backup_database_name = element.database_name;
                    String backup_table_name = element.table_name;
                    DatabasePtr database = DatabaseCatalog::instance().getDatabase(backup_database_name, context);
                    if (database == nullptr)
                        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "database {} does not exist.", backup_database_name);
                    std::shared_ptr<DatabaseCnch> cnch_database = std::dynamic_pointer_cast<DatabaseCnch>(database);
                    if (cnch_database == nullptr)
                        throw Exception(ErrorCodes::BACKUP_JOB_UNSUPPORTED, "database {} is not a Cnch database.", backup_database_name);

                    StoragePtr table = database->tryGetTable(backup_table_name, context);
                    if (table == nullptr)
                        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist.", backup_table_name);

                    std::shared_ptr<StorageCnchMergeTree> cnch_merge_tree = dynamic_pointer_cast<StorageCnchMergeTree>(table);
                    if (!cnch_merge_tree)
                        throw Exception(
                            ErrorCodes::BACKUP_JOB_UNSUPPORTED, "The type of table {} does not support backup now.", backup_table_name);

                    context->checkAccess(AccessFlags(AccessType::SELECT), backup_database_name, backup_table_name);
                    backup_tables.emplace_back(std::move(table));
                }
                else
                {
                    String restore_database_name = element.new_database_name.empty() ? element.database_name : element.new_database_name;
                    String restore_table_name = element.new_table_name.empty() ? element.table_name : element.new_table_name;

                    DatabasePtr database = DatabaseCatalog::instance().getDatabase(restore_database_name, context);
                    // If restore database does not exist, we have to create a new one therefore check CREATE_DATABASE access.
                    if (database == nullptr)
                        context->checkAccess(AccessFlags(AccessType::CREATE_DATABASE), restore_database_name);
                    else
                    {
                        std::shared_ptr<DatabaseCnch> cnch_database = std::dynamic_pointer_cast<DatabaseCnch>(database);
                        // If restore database exists, we requires it's a cnch database
                        if (cnch_database == nullptr)
                            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "database {} is not Cnch database.", restore_database_name);
                        // If restore table does not exist, we have to check CREATE_TABLE access
                        StoragePtr table = database->tryGetTable(restore_table_name, context);
                        if (table == nullptr)
                            context->checkAccess(AccessFlags(AccessType::CREATE_TABLE), restore_database_name, restore_table_name);
                    }                    
                }
                break;
            }
            case ASTBackupQuery::ElementType::DATABASE: {
                context->checkAccess(AccessType::SHOW_TABLES, element.new_database_name);

                DatabasePtr database;
                if (backup_query.kind == ASTBackupQuery::BACKUP)
                {
                    database = DatabaseCatalog::instance().getDatabase(element.database_name, context);
                    if (database == nullptr)
                        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "database {} does not exist.", element.database_name);
                    std::shared_ptr<DatabaseCnch> cnch_database = std::dynamic_pointer_cast<DatabaseCnch>(database);
                    if (cnch_database == nullptr)
                        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "database {} is not Cnch database.", element.database_name);

                    // If database exists, then we check table
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

                        std::shared_ptr<StorageCnchMergeTree> cnch_merge_tree = dynamic_pointer_cast<StorageCnchMergeTree>(table);
                        if (!cnch_merge_tree)
                            throw Exception(
                                ErrorCodes::BACKUP_JOB_UNSUPPORTED,
                                "The type of table {} does not support backup now.",
                                table->getTableName());

                        context->checkAccess(AccessFlags(AccessType::SELECT), database->getDatabaseName(), table->getTableName());
                        backup_tables.emplace_back(std::move(table));
                    }
                }
                else
                {
                    String restore_database_name = element.new_table_name.empty() ? element.database_name : element.new_database_name;

                    database = DatabaseCatalog::instance().getDatabase(restore_database_name, context);
                    // If restore database does not exist, we have to create a new one therefore check CREATE_DATABASE access.
                    if (database == nullptr)
                    {
                        context->checkAccess(AccessFlags(AccessType::CREATE_DATABASE), restore_database_name);
                        continue;
                    }
                    else
                    {
                        std::shared_ptr<DatabaseCnch> cnch_database = std::dynamic_pointer_cast<DatabaseCnch>(database);
                        if (cnch_database == nullptr)
                            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "database {} already exists and is not Cnch database.", element.database_name);
                    }
                }
            }
        }
    }

    // Check there are no running mutation tasks on backup tables
    Catalog::CatalogPtr catalog = context->getCnchCatalog();
    for (StoragePtr & backup_table : backup_tables)
    {
        if (!catalog->getAllMutations(backup_table->getStorageID()).empty())
            throw Exception(
                ErrorCodes::BACKUP_JOB_CREATE_FAILED,
                "Backup job create failed because there are running mutation tasks on table {}",
                backup_table->getName());
    }

    // Check backup_disk exists
    BackupDiskInfo backup_info = BackupDiskInfo::fromAST(*backup_query.backup_disk);
    DiskPtr backup_disk;
    try
    {
        backup_disk = context->getDisk(backup_info.disk_name);
    }
    catch (...)
    {
        throw Exception(ErrorCodes::UNKNOWN_DISK, "Disk {} does not exist", backup_info.disk_name);
    }

    if (std::filesystem::path(backup_info.backup_dir).is_absolute())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Given backup directory is an absolute path, it should be relative to disk.");

    if (backup_query.kind == ASTBackupQuery::BACKUP && !backup_disk->isDirectoryEmpty(backup_info.backup_dir))
        throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup path has to be empty.");
}


BlockIO InterpreterBackupQuery::execute()
{
    check();

    BackupsWorkerPtr backups_worker = std::make_shared<BackupsWorker>(query_ptr, context);
    BackupResult backup_result = backups_worker->start();

    BlockIO res_io;
    res_io.pipeline.init(Pipe(std::make_shared<SourceFromSingleChunk>(getResultRow(backup_result))));
    return res_io;
}

}
