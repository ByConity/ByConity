#pragma once

#include <Catalog/Catalog.h>
#include <DaemonManager/DaemonJob.h>
#include <Common/Stopwatch.h>

namespace DB::DaemonManager
{

const size_t MAX_RETRY = 3;
// unit seconds, three days
// After MAX_TTL_TIME, we will clear FAILED and FINISHED backup tasks from catalog
const size_t MAX_TTL_TIME = 259200;

class DaemonJobBackup : public DaemonJob
{
public:
    explicit DaemonJobBackup(ContextMutablePtr global_context_)
        : DaemonJob(global_context_, CnchBGThreadType::Backup), catalog(getContext()->getCnchCatalog())
    {
    }
    bool executeImpl() override;

private:
    std::shared_ptr<Catalog::Catalog> catalog;
    LoggerPtr logger = getLogger("BackupDaemon");

    std::unordered_map<String, size_t> retry_connect_tasks;
    std::unordered_map<String, Stopwatch> finished_tasks;
};

}
