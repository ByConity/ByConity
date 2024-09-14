#include <optional>
#include <Backups/BackupDiskInfo.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntriesWriter.h>
#include <Backups/BackupEntryFromFile.h>
#include <Backups/BackupRenamingConfig.h>
#include <Backups/BackupSettings.h>
#include <Backups/BackupSnapshotMaker.h>
#include <Backups/BackupStatus.h>
#include <Backups/BackupUtils.h>
#include <Backups/BackupsWorker.h>
#include <Backups/IBackupEntry.h>
#include <Backups/MetaRestorer.h>
#include <Backups/renameInCreateQuery.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <common/logger_useful.h>
#include <common/scope_guard_safe.h>


namespace CurrentMetrics
{
extern const Metric BackupsThreads;
extern const Metric BackupsThreadsActive;
extern const Metric RestoreThreads;
extern const Metric RestoreThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int BACKUP_JOB_CREATE_FAILED;
    extern const int BACKUP_JOB_NOT_EXIST;
    extern const int BACKUP_STATUS_ERROR;
}

BackupsWorker::BackupsWorker(const ASTPtr & backup_or_restore_query_, ContextMutablePtr context_)
    : backup_or_restore_query(backup_or_restore_query_)
    , context(context_)
{
}

BackupResult BackupsWorker::start()
{
    const ASTBackupQuery & backup_query = backup_or_restore_query->as<const ASTBackupQuery &>();
    // 1. Generate back up uuid
    backup_id = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());

    if (backup_query.kind == ASTBackupQuery::Kind::BACKUP)
        return startMakingBackup();
    else
        return startRestoring();
}

BackupResult BackupsWorker::startMakingBackup()
{
    const ASTBackupQuery & backup_query = backup_or_restore_query->as<const ASTBackupQuery &>();
    backup_settings = BackupSettings::fromBackupQuery(backup_query, context);

    // 1. create snapshots in case backup table is dropped
    BackupSnapshotMaker::makeSnapshots(backup_query, backup_id, context);

    // 2. create backup job
    auto server_address = context->getHostWithPorts().getRPCAddress();
    try
    {
        context->getCnchCatalog()->createBackupJob(
            backup_id,
            context->getCurrentTransactionID(),
            BackupStatus::ACCEPTED,
            serializeAST(*backup_or_restore_query),
            server_address,
            backup_settings.enable_auto_recover);
    }
    catch (...)
    {
        BackupSnapshotMaker::clearSnapshots(backup_query, backup_id, context);
        throw;
    }

    if (backup_settings.async)
    {
        // 3. Task will be scheduled by daemonManager in async mode
        return {backup_id, BackupStatus::ACCEPTED, "Job status can be checked by table system.cnch_backups using id"};
    }
    else
    {
        // 3. do backup job synchronously
        doBackup(backup_id, backup_or_restore_query, context);
        return {backup_id, BackupStatus::COMPLETED, "Job can be checked by table system.cnch_backups using id"};
    }
}

BackupResult BackupsWorker::startRestoring()
{
    const ASTBackupQuery & restore_query = backup_or_restore_query->as<const ASTBackupQuery &>();
    backup_settings = BackupSettings::fromBackupQuery(restore_query, context);

    // 1. create backup job
    auto server_address = context->getHostWithPorts().getRPCAddress();
    context->getCnchCatalog()->createBackupJob(
        backup_id,
        context->getCurrentTransactionID(),
        BackupStatus::ACCEPTED,
        serializeAST(*backup_or_restore_query),
        server_address,
        backup_settings.enable_auto_recover);

    if (backup_settings.async)
        // Will be scheduled by daemonManager
        return {backup_id, BackupStatus::ACCEPTED, "Job status can be checked by table system.cnch_backups using id"};
    else
    {
        doRestore(backup_id, backup_or_restore_query, context);
        return {backup_id, BackupStatus::COMPLETED, "Job can be checked by table system.cnch_backups using id"};
    }
}

static void finishBackupJob(
    BackupTaskPtr & backup_task,
    BackupStatus status,
    const ContextPtr & local_context,
    std::optional<String> error_msg)
{
    backup_task->updateStatus(status, local_context, error_msg);
    local_context->getGlobalContext()->removeRunningBackupTask(backup_task->getId());
}

bool checkBackupTaskStatus(
    const String & backup_id, BackupStatus & status, BackupSettings & backup_settings, ContextMutablePtr & local_context)
{
    switch (status)
    {
        // If status is accepted here, backup task is running synchronously
        case BackupStatus::ACCEPTED:
        // If status is scheduling here, backup task is scheduled by daemon manager asynchronously for the first time
        case BackupStatus::SCHEDULING:
            return true;
        // If status is rescheduling here, backup task is failed for the previous server and scheduled again
        // If there is a current running backup task, just ignore this reschedule.
        case BackupStatus::RESCHEDULING:
            return backup_settings.enable_auto_recover && !local_context->hasRunningBackupTask();
        case BackupStatus::RUNNING:
            throw Exception(ErrorCodes::BACKUP_STATUS_ERROR, "Backup task {} is running, can't be scheduled again.", backup_id);
        case BackupStatus::COMPLETED:
            throw Exception(ErrorCodes::BACKUP_STATUS_ERROR, "Backup task {} has completed, can't be scheduled again.", backup_id);
        case BackupStatus::FAILED:
            throw Exception(ErrorCodes::BACKUP_STATUS_ERROR, "Backup task {} has failed, can't be scheduled again.", backup_id);
        case BackupStatus::ABORTED:
            throw Exception(ErrorCodes::BACKUP_STATUS_ERROR, "Backup task {} has been aborted, can't be scheduled again.", backup_id);
        default:
            throw Exception(ErrorCodes::BACKUP_STATUS_ERROR, "Unknown backup status {}", status);
    }
}

bool isBackupEntryFileUploadSuccess(DiskPtr & backup_disk, String & backup_dir)
{
    String backup_entry_meta_path = backup_dir + "/" + getBackupEntryMetaFilePathInBackup();
    String backup_entry_file_path = backup_dir + "/" + getBackupEntryFilePathInBackup();
    if (!backup_disk->fileExists(backup_entry_meta_path) || !backup_disk->fileExists(backup_entry_file_path))
        return false;

    auto meta_file_buffer = backup_disk->readFile(backup_entry_meta_path);
    size_t file_size;
    readIntBinary(file_size, *meta_file_buffer);

    return backup_disk->getFileSize(backup_entry_file_path) == file_size;
}

void BackupsWorker::doBackup(const String & backup_id, ASTPtr backup_query_ptr, ContextMutablePtr & local_context)
{
    const ASTBackupQuery & backup_query = backup_query_ptr->as<const ASTBackupQuery &>();
    std::shared_ptr<Catalog::Catalog> catalog = local_context->getCnchCatalog();
    BackupTaskModel task_model = catalog->tryGetBackupJob(backup_id);
    if (!task_model)
        throw Exception(ErrorCodes::BACKUP_JOB_NOT_EXIST, "Backup job {} does not exist.", backup_id);

    BackupTaskPtr backup_task = std::make_shared<BackupTask>(task_model);
    BackupSettings backup_settings = BackupSettings::fromBackupQuery(backup_query, local_context);
    BackupStatus status = getBackupStatus(backup_task->getStatus());
    if (!checkBackupTaskStatus(backup_id, status, backup_settings, local_context))
        return;

    // reentrant
    if (!local_context->trySetRunningBackupTask(backup_id))
        return;
    LoggerPtr logger = getLogger("BackupsWorker");
    auto backup_info = BackupDiskInfo::fromAST(*backup_query.backup_disk);
    DiskPtr backup_disk = local_context->getDisk(backup_info.disk_name);
    try
    {
        if (!local_context->getCurrentTransaction())
        {
            local_context = getContextWithNewTransaction(local_context, true);
        }
        SCOPE_EXIT_SAFE({ local_context->getCnchTransactionCoordinator().finishTransaction(local_context->getCurrentTransactionID()); });
        trySetVirtualWarehouseAndWorkerGroup(backup_settings.virtual_warehouse, local_context);

        bool restart_from_beginning = false;
        // Check previous progress
        if (status == BackupStatus::RESCHEDULING)
        {
            // If backup entry has not been uploaded successfully, restart the task from beginning
            if (!isBackupEntryFileUploadSuccess(backup_disk, backup_info.backup_dir))
            {
                restart_from_beginning = true;
            }
        }

        backup_task->updateStatus(BackupStatus::RUNNING, local_context, std::nullopt);
        LOG_INFO(logger, "Backup task {} start running", backup_id);

        if (status == BackupStatus::ACCEPTED || status == BackupStatus::SCHEDULING || restart_from_beginning)
        {
            /// 1. Prepare backup entries.
            BackupEntriesCollector backup_entries_collector(
                backup_query, backup_task, local_context, backup_settings.max_backup_entries, task_model->create_time());
            BackupEntries & backup_entries = backup_entries_collector.collect();

            // 2. Write backup entries
            BackupEntriesWriter backup_writer(backup_query, backup_task, local_context);
            backup_writer.write(backup_entries, true);
        }
        // Start from intermediate
        else
        {
            String backup_entry_file = backup_info.backup_dir + "/" + getBackupEntryFilePathInBackup();
            BackupEntriesWriter backup_writer(backup_query, backup_task, local_context);
            backup_writer.write(backup_entry_file);
        }
    }
    catch (Exception & e)
    {
        /// Something bad happened, the backup has not built.
        tryLogCurrentException(logger, "Backup task failed because: ");
        BackupSnapshotMaker::clearSnapshots(backup_query, backup_id, local_context);
        finishBackupJob(backup_task, BackupStatus::FAILED, local_context, e.message());
        // clear backup directory
        if (backup_disk->exists(backup_info.backup_dir))
            backup_disk->removeRecursive(backup_info.backup_dir);
        throw;
    }
    // 3. clear snapshots
    BackupSnapshotMaker::clearSnapshots(backup_query, backup_id, local_context);
    finishBackupJob(backup_task, BackupStatus::COMPLETED, local_context, std::nullopt);
    LOG_INFO(logger, "Backup task {} completed successfully", backup_id);
}

void BackupsWorker::doRestore(const String & backup_id, ASTPtr restore_query_ptr, ContextMutablePtr & local_context)
{
    const ASTBackupQuery & restore_query = restore_query_ptr->as<const ASTBackupQuery &>();
    std::shared_ptr<Catalog::Catalog> catalog = local_context->getCnchCatalog();
    BackupTaskModel task_model = catalog->tryGetBackupJob(backup_id);
    if (!task_model)
        throw Exception(ErrorCodes::BACKUP_JOB_NOT_EXIST, "Restore job {} does not exist.", backup_id);

    BackupTaskPtr backup_task = std::make_shared<BackupTask>(task_model);
    BackupSettings backup_settings = BackupSettings::fromBackupQuery(restore_query, local_context);
    BackupStatus status = getBackupStatus(backup_task->getStatus());
    if (!checkBackupTaskStatus(backup_id, status, backup_settings, local_context))
        return;

    // reentrant
    if (!local_context->trySetRunningBackupTask(backup_id))
        return;
    LoggerPtr logger = getLogger("BackupsWorker");
    try
    {
        if (!local_context->getCurrentTransaction())
        {
            local_context = getContextWithNewTransaction(local_context, false);
        }
        SCOPE_EXIT_SAFE({ local_context->getCnchTransactionCoordinator().finishTransaction(local_context->getCurrentTransactionID()); });
        trySetVirtualWarehouseAndWorkerGroup(backup_settings.virtual_warehouse, local_context);

        // Check previous progress
        std::shared_ptr<std::set<DatabaseAndTableName>> finished_tables;
        if (status == BackupStatus::RESCHEDULING && task_model->finished_tables_size() > 0)
        {
            finished_tables = std::make_shared<std::set<DatabaseAndTableName>>();
            for (const auto & table_item : task_model->finished_tables())
            {
                int dot_pos = table_item.find_last_of('.');
                finished_tables->emplace(table_item.substr(0, dot_pos), table_item.substr(dot_pos + 1));
            }
        }
        // Update restore task status
        backup_task->updateStatus(BackupStatus::RUNNING, local_context, std::nullopt);
        LOG_INFO(logger, "Restore task {} start running", backup_id);

        auto backup_info = BackupDiskInfo::fromAST(*restore_query.backup_disk);

        auto renaming_config = std::make_shared<BackupRenamingConfig>();
        renaming_config->setFromBackupQueryElements(restore_query.elements);

        for (const auto & element : restore_query.elements)
        {
            switch (element.type)
            {
                case ASTBackupQuery::ElementType::TABLE: {
                    restoreTable(
                        {element.database_name, element.table_name},
                        backup_task,
                        backup_info,
                        local_context,
                        renaming_config,
                        element.partitions,
                        finished_tables);
                    break;
                }
                case ASTBackupQuery::ElementType::DATABASE: {
                    restoreDatabase(
                        element.database_name,
                        backup_task,
                        element.except_tables,
                        backup_info,
                        local_context,
                        renaming_config,
                        finished_tables);
                    break;
                }
            }
        }
    }
    catch (Exception & e)
    {
        /// Something bad happened, the restore has not built.
        tryLogCurrentException(logger, "Restore task failed because: ");
        finishBackupJob(backup_task, BackupStatus::FAILED, local_context, e.message());
        throw;
    }
    finishBackupJob(backup_task, BackupStatus::COMPLETED, local_context, std::nullopt);
    LOG_INFO(logger, "Restore task {} completed successfully", backup_id);
}

void BackupsWorker::restoreTable(
    const DatabaseAndTableName & table_name,
    BackupTaskPtr & backup_task,
    const BackupDiskInfo & backup_info,
    const ContextPtr & local_context,
    const BackupRenamingConfigPtr & renaming_config,
    std::optional<ASTs> partitions,
    std::shared_ptr<std::set<DatabaseAndTableName>> & finished_tables)
{
    if (finished_tables && finished_tables->contains(table_name))
        return;

    // 1. Restore table metadata
    checkBackupTaskNotAborted(backup_task->getId(), local_context);
    backup_task->updateProgress("Restore meta of table " + table_name.first + "." + table_name.second, local_context);

    DiskPtr backup_disk = local_context->getDisk(backup_info.disk_name);
    ASTPtr create_table_query = readCreateQueryFromBackup(table_name, backup_disk, backup_info.backup_dir);
    // Rename database and table in backup DDL
    auto new_create_query
        = typeid_cast<std::shared_ptr<ASTCreateQuery>>(renameInCreateQuery(create_table_query, renaming_config, local_context));
  // uuid will be used to search backup data path
    String table_uuid = UUIDHelpers::UUIDToString(new_create_query->uuid);
    // Reset uuid to generate a new one
    new_create_query->uuid = UUIDHelpers::generateV4();

    std::optional<std::vector<ASTPtr>> previous_versions = readTableHistoryFromBackup(table_name, backup_disk, backup_info.backup_dir);
    if (previous_versions && !previous_versions->empty())
    {
        // Rename table and reset uuid, reassign new definition
        for (size_t i = 0; i < previous_versions->size(); i++)
        {
            auto new_history_table
                = typeid_cast<std::shared_ptr<ASTCreateQuery>>(renameInCreateQuery(create_table_query, renaming_config, local_context));
            new_history_table->uuid = new_create_query->uuid;
        }
    }

    MetaRestorer::restoreTable(new_create_query, previous_versions, local_context);

    // 2. Restore data
    checkBackupTaskNotAborted(backup_task->getId(), local_context);
    backup_task->updateProgress("Restore data of table " + table_name.first + "." + table_name.second, local_context);

    DatabaseAndTableName new_table_name{new_create_query->getDatabase(), new_create_query->getTable()};
    String data_path_in_backup = fs::path(backup_info.backup_dir) / getDataPathInBackup(table_uuid);
    // Create new transcation to restore data
    ContextMutablePtr new_context = getContextWithNewTransaction(local_context, false);
    SCOPE_EXIT_SAFE({ new_context->getCnchTransactionCoordinator().finishTransaction(new_context->getCurrentTransactionID()); });
    // We have to use new context to get database and table here, otherwise new created table will be invisible
    DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(new_table_name.first, new_context);
    if (!database)
        throw Exception(
            "Database " + backQuoteIfNeed(new_table_name.first) + " for restoring is not exist and create failed.",
            ErrorCodes::LOGICAL_ERROR);

    StoragePtr storage = database->tryGetTable(new_table_name.second, new_context);
    if (!storage)
        throw Exception(
            "Table " + backQuoteIfNeed(table_name.first) + "." + backQuoteIfNeed(table_name.second)
                + " for restoring is not exist and create failed",
            ErrorCodes::LOGICAL_ERROR);
    storage->restoreDataFromBackup(backup_task, backup_disk, data_path_in_backup, new_context, partitions);

    backup_task->addFinishedTables(table_name.first + "." + table_name.second, local_context);
}

Strings getTableMetaFileNameFromBackupDisk(const DiskPtr & backup_disk, const String & backup_dir, const String & database_name)
{
    Strings table_metadata_filenames;
    backup_disk->listFiles(backup_dir + "metadata/" + escapeForFileName(database_name) + "/", table_metadata_filenames);

    if (backup_disk->getInnerType() == DiskType::Type::ByteS3)
        for (String & table_metadata_filename : table_metadata_filenames)
            table_metadata_filename = fs::path(table_metadata_filename).filename();

    return table_metadata_filenames;
}

void BackupsWorker::restoreDatabase(
    const String & database_name,
    BackupTaskPtr & backup_task,
    std::set<DatabaseAndTableName> except_tables,
    const BackupDiskInfo & backup_info,
    const ContextPtr & local_context,
    const BackupRenamingConfigPtr & renaming_config,
    std::shared_ptr<std::set<DatabaseAndTableName>> & finished_tables)
{
    // 1. Restore database metadata
    DiskPtr backup_disk = local_context->getDisk(backup_info.disk_name);
    ASTPtr create_database_query = readCreateQueryFromBackup(database_name, backup_disk, backup_info.backup_dir);
    auto new_create_query
        = typeid_cast<std::shared_ptr<ASTCreateQuery>>(renameInCreateQuery(create_database_query, renaming_config, local_context));

    // check restore database is exist or not
    new_create_query->if_not_exists = true;
    new_create_query->uuid = UUIDHelpers::Nil;
    MetaRestorer::restoreDatabase(new_create_query, local_context);

    // 2. Traverse backup path to search table to restore
    const String & new_database_name = new_create_query->getDatabase();

    Strings table_metadata_filenames = getTableMetaFileNameFromBackupDisk(backup_disk, backup_info.backup_dir, database_name);
    std::unordered_set<String> except_table_names;
    if (!except_tables.empty())
    {
        for (const auto & except_table : except_tables)
        {
            if (except_table.first == new_database_name)
                except_table_names.emplace(except_table.second);
        }
    }

    // For table.sql and table.history, we handle it only once
    std::unordered_set<String> handled_tables;
    for (const String & table_metadata_filename : table_metadata_filenames)
    {
        String table_name = unescapeForFileName(fs::path{table_metadata_filename}.stem());
        if (handled_tables.contains(table_name))
            continue;

        handled_tables.emplace(table_name);
        if (!except_table_names.contains(table_name))
            restoreTable(
                {database_name, table_name}, backup_task, backup_info, local_context, renaming_config, std::nullopt, finished_tables);
    }
}

}
