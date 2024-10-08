#pragma once

#include <optional>
#include <Core/Types.h>
#include <Parsers/IAST.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
class ASTBackupQuery;

/// Settings specified in the "SETTINGS" clause of a BACKUP query.
struct BackupSettings
{
    /// Compression method and level for writing the backup (when applicable).
    String compression_method; /// "" means default method
    int compression_level = -1; /// -1 means default level

    /// Whether the BACKUP command must return immediately without waiting until the backup has completed.
    bool async = true;

    // virtual warehouse responsible for the copy file task
    String virtual_warehouse = "vw_write";

    // Allow recover from unexpected error automatically and rescheduled by daemon manager
    bool enable_auto_recover = true;

    // Max backup entries to keep
    // Assume one backup entry is about 200 Byte, we keep 10M entries as a batch, which is up to 2GB.
    UInt64 max_backup_entries = 10000000;

    static BackupSettings fromBackupQuery(const ASTBackupQuery & query, ContextMutablePtr & context);
    void copySettingsToQuery(ASTBackupQuery & query) const;
};

}
