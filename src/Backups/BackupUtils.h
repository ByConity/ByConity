#pragma once

#include <Backups/BackupStatus.h>
#include <Catalog/Catalog.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTBackupQuery.h>
#include <Protos/data_models.pb.h>
#include <Interpreters/executeSubQuery.h>

#define BACKUP_SCHEMA_VERSION 1

namespace DB
{

struct BackupTask
{
    std::shared_ptr<Protos::DataModelBackupTask> task;
    // Used for CAS, store the value before update
    String expected_value;
    std::mutex mtx;

    explicit BackupTask(std::shared_ptr<Protos::DataModelBackupTask> task_) : task(task_), expected_value(task_->SerializeAsString()) { }

    String getId() const { return task->id(); }

    UInt64 getStatus() const { return task->status(); }

    void updateCatalog(const ContextPtr & context)
    {
        context->getCnchCatalog()->updateBackupJobCAS(task, expected_value);
        expected_value = task->SerializeAsString();
    }

    void updateStatus(BackupStatus status, const ContextPtr & context, std::optional<String> error_msg)
    {
        task->set_status(static_cast<UInt64>(status));
        if (error_msg)
            task->set_last_error(*error_msg);
        updateCatalog(context);
    }

    void updateProgress(String progress, const ContextPtr & context)
    {
        task->set_progress(progress);
        updateCatalog(context);
    }

    void updateStatusAndProgress(BackupStatus status, String progress, const ContextPtr & context)
    {
        task->set_status(static_cast<UInt64>(status));
        task->set_progress(progress);
        updateCatalog(context);
    }

    void updateProgressWithLock(String progress, const ContextPtr & context)
    {
        std::lock_guard<std::mutex> lock(mtx);
        updateProgress(progress, context);
    }

    void updateTotalBackupBytes(size_t total_size, const ContextPtr & context)
    {
        task->set_total_backup_size(total_size);
        updateCatalog(context);
    }

    void addFinishedTables(String finished_table, const ContextPtr & context)
    {
        *task->add_finished_tables() = finished_table;
        updateCatalog(context);
    }
};

using BackupTaskPtr = std::shared_ptr<BackupTask>;
using BackupTasks = std::vector<BackupTaskPtr>;

void checkBackupTaskNotAborted(const String & backup_id, const ContextPtr & context);

String getDataPathInBackup(const String & table_uuid);

String getDataPathInBackup(const IAST & create_query);

String getDeleteFilesPathInBackup(String data_path_in_backup);

String getDeleteFilesPathInBackup(const IAST & create_query);

String getPartFilesPathInBackup(String data_path_in_backup);

String getPartFilesPathInBackup(const IAST & create_query);

String getMetaHistoryPathInBackup(const IAST & create_query);

String getMetaHistoryPathInBackup(const DatabaseAndTableName & table_name);

String getMetadataPathInBackup(const DatabaseAndTableName & table_name);

String getMetadataPathInBackup(const String & database_name);

String getMetadataPathInBackup(const IAST & create_query);

String getBackupEntryMetaFilePathInBackup();

String getBackupEntryFilePathInBackup();

ASTPtr readCreateQueryFromBackup(const String & database_name, const DiskPtr & backup_disk, const String & base_backup_path);

ASTPtr readCreateQueryFromBackup(const DatabaseAndTableName & table_name, const DiskPtr & backup_disk, const String & base_backup_path);

std::optional<std::vector<ASTPtr>> readTableHistoryFromBackup(const DatabaseAndTableName & table_name, const DiskPtr & backup_disk, const String & base_backup_path);

}
