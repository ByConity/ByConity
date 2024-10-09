#include <Backups/BackupStatus.h>
#include <Common/Exception.h>
#include <common/types.h>
#include <common/range.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BACKUP_STATUS_ERROR;
}

std::string_view toString(BackupStatus backup_status)
{
    switch (backup_status)
    {
        case BackupStatus::ACCEPTED:
            return "ACCEPTED";
        case BackupStatus::SCHEDULING:
            return "SCHEDULING";
        case BackupStatus::RUNNING:
            return "RUNNING";
        case BackupStatus::COMPLETED:
            return "COMPLETED";
        case BackupStatus::FAILED:
            return "FAILED";
        case BackupStatus::RESCHEDULING:
            return "RESCHEDULING";
        case BackupStatus::ABORTED:
            return "ABORTED";
        default:
            break;
    }
    throw Exception(ErrorCodes::BACKUP_STATUS_ERROR, "Unexpected backup status: {}", static_cast<int>(backup_status));
}

const std::vector<std::pair<String, Int8>> & getBackupStatusEnumValues()
{
    static const std::vector<std::pair<String, Int8>> values = []
    {
        std::vector<std::pair<String, Int8>> res;
        for (auto status : collections::range(BackupStatus::MAX))
            res.emplace_back(toString(status), static_cast<Int8>(status));
        return res;
    }();
    return values;
}

BackupStatus getBackupStatus(UInt64 status)
{
    switch (status)
    {
        case 0:
            return BackupStatus::ACCEPTED;
        case 1:
            return BackupStatus::SCHEDULING;
        case 2:
            return BackupStatus::RUNNING;
        case 3:
            return BackupStatus::COMPLETED;
        case 4:
            return BackupStatus::FAILED;
        case 5:
            return BackupStatus::RESCHEDULING;
        case 6:
            return BackupStatus::ABORTED;
        default:
            throw Exception(ErrorCodes::BACKUP_STATUS_ERROR, "Unexpected backup status: {}", status);
    }
}
}
