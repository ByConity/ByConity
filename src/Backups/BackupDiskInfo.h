#pragma once

#include <Core/Field.h>


namespace DB
{
class IAST;

/// Information about backup disk and backup dir
struct BackupDiskInfo
{
    String backup_engine_name;
    String disk_name;
    String backup_dir;

    String toString() const;
    static BackupDiskInfo fromString(const String & str);
    static BackupDiskInfo fromAST(const IAST & ast);
};

}
