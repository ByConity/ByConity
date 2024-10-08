#pragma once

#include <common/types.h>

namespace DB
{
enum class BackupStatus
{
    ACCEPTED = 0,
    SCHEDULING = 1,
    RUNNING = 2,
    COMPLETED = 3,
    FAILED = 4,
    // Failed once and rescheduled by daemon manager
    RESCHEDULING = 5,
    ABORTED = 6,

    MAX,
};

std::string_view toString(BackupStatus backup_status);

/// Returns vector containing all values of BackupStatus and their string representation,
/// which is used to create DataTypeEnum8 to store those statuses.
const std::vector<std::pair<String, Int8>> & getBackupStatusEnumValues();

BackupStatus getBackupStatus(UInt64 status);
}
