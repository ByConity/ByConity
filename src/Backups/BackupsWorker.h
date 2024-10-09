#pragma once

#include <unordered_map>
#include <Backups/BackupDiskInfo.h>
#include <Backups/BackupRenamingConfig.h>
#include <Backups/BackupSettings.h>
#include <Backups/BackupStatus.h>
#include <Catalog/Catalog.h>
#include <Core/UUID.h>
#include <Databases/IDatabase.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Common/ThreadPool.h>


namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{
constexpr UInt64 MAX_SNAPSHOT_TTL = 999;
using DatabaseAndTableName = std::pair<String, String>;

// <database_uuid, snapshot_name>
using BackupSnapshot = std::pair<UUID, String>;
using BackupSnapshots = std::vector<std::pair<UUID, String>>;

// <backup_job_id, backup_status>
struct BackupResult
{
    String backup_id;
    BackupStatus status;
    String info;
};

struct BackupSettings;
struct QualifiedTableName;

/// Manager of backups and restores
class BackupsWorker
{
public:
    BackupsWorker(const ASTPtr & backup_or_restore_query, ContextMutablePtr context);

    /// Starts executing a BACKUP or RESTORE query. Returns ID and status of the operation.
    BackupResult start();

    static void doRestore(const String & backup_id, ASTPtr restore_query_ptr, ContextMutablePtr & context);

    static void doBackup(const String & backup_id, ASTPtr backup_query_ptr, ContextMutablePtr & context);

private:
    BackupResult startMakingBackup();

    BackupResult startRestoring();

    static void restoreTable(
        const DatabaseAndTableName & table_name,
        BackupTaskPtr & backup_id,
        const BackupDiskInfo & backup_info,
        const ContextPtr & local_context,
        const BackupRenamingConfigPtr & renaming_config,
        std::optional<ASTs> partitions,
        std::shared_ptr<std::set<DatabaseAndTableName>> & finished_tables);

    static void restoreDatabase(
        const String & database_name,
        BackupTaskPtr & backup_id,
        std::set<DatabaseAndTableName> except_tables,
        const BackupDiskInfo & backup_info,
        const ContextPtr & local_context,
        const BackupRenamingConfigPtr & renaming_config,
        std::shared_ptr<std::set<DatabaseAndTableName>> & finished_tables);

    const ASTPtr & backup_or_restore_query;

    ContextMutablePtr context;

    BackupSettings backup_settings;

    String backup_id;
};

}
