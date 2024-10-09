#pragma once

#include <memory>
#include <Backups/BackupDiskInfo.h>
#include <Backups/BackupRenamingConfig.h>
#include <Backups/BackupUtils.h>
#include <Backups/IBackupEntry.h>
#include <Catalog/Catalog.h>
#include <Databases/IDatabase.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTBackupQuery.h>
#include <Common/ThreadPool.h>

namespace DB
{
/// Write backup entries for all databases and tables which should be put to a backup.
class BackupEntriesWriter : private boost::noncopyable
{
public:
    BackupEntriesWriter(const ASTBackupQuery & backup_query_, BackupTaskPtr & backup_task_, const ContextPtr & context_);

    ~BackupEntriesWriter() = default;

    void addBackupEntriesToRocksDB(const String & entry_dir, const BackupEntries & backup_entries, size_t & total_bytes) const;
    void uploadBackupEntryFile(
        DiskPtr & backup_disk, const String & backup_dir, const BackupEntries & backup_entries) const;
    /// Write backup entries
    void write(BackupEntries & backup_entries, bool need_upload_entry_file) const;

    void getBackupEntriesFromRemote(DiskPtr & backup_disk, String & backup_entry_remote_path, BackupEntries & backup_entries) const;
    void loadBackupEntriesFromRocksDB(const String & entry_dir, const DiskPtr & backup_disk, BackupEntries & backup_entries) const;
    void write(String & backup_entry_remote_path) const;

    const ASTBackupQuery & backup_query;
    BackupDiskInfo backup_info;
    BackupTaskPtr & backup_task;
    ContextPtr context;
};

}
