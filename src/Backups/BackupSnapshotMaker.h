#pragma once

#include <Backups/BackupsWorker.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{
namespace BackupSnapshotMaker
{
    BackupSnapshot createSnapshotForTable(DatabasePtr database, StoragePtr table, const String & backup_id, const ContextPtr & context);

    BackupSnapshots makeSnapshots(const ASTBackupQuery & backup_query, const String & backup_id, const ContextPtr & context);

    void clearSnapshots(const ASTBackupQuery & backup_query, const String & backup_id, const ContextPtr & context);

    void removeSnapshot(const ContextPtr & context, BackupSnapshot snapshot);
};

}
