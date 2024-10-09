#pragma once

#include <Backups/BackupRenamingConfig.h>
#include <Backups/BackupUtils.h>
#include <Backups/IBackupEntry.h>
#include <Catalog/Catalog.h>
#include <Core/QualifiedTableName.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTBackupQuery.h>

namespace DB
{
/// Collects backup entries for all databases and tables which should be put to a backup.
class BackupEntriesCollector : private boost::noncopyable
{
public:
    BackupEntriesCollector(
        const ASTBackupQuery & backup_query_,
        BackupTaskPtr & backup_task_,
        const ContextPtr & context_,
        size_t max_backup_entries_,
        UInt64 snapshot_ts_);

    ~BackupEntriesCollector() = default;

    /// Collects backup entries and returns the result.
    BackupEntries & collect();

    void collectTableEntries(
        const DatabaseAndTableName & database_table, const BackupRenamingConfigPtr & renaming_config, std::optional<ASTs> partitions);

    void collectDatabaseEntries(
        const String & database_name, std::set<DatabaseAndTableName> except_tables, const BackupRenamingConfigPtr & renaming_config);

    const ASTBackupQuery & backup_query;
    BackupTaskPtr & backup_task;
    ContextPtr context;
    size_t max_backup_entries;
    UInt64 snapshot_ts;

    BackupEntries backup_entries;
};

}
