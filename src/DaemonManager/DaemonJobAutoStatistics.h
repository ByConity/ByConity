#pragma once

#include <Common/Logger.h>
#include <chrono>
#include <DaemonManager/DaemonJob.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsManager.h>


namespace DB::ErrorCodes
{
extern const int SYSTEM_ERROR;

}

namespace DB::DaemonManager
{

namespace AutoStatsImpl
{
    struct ServerCollectorInfo
    {
        HostWithPorts host_with_ports;
    };
}

class DaemonJobAutoStatistics : public DaemonJob
{
public:
    explicit DaemonJobAutoStatistics(ContextMutablePtr global_context_) : DaemonJob(global_context_, CnchBGThreadType::AutoStatistics) { }
    bool executeImpl() override;
    static HostWithPortsVec getServerList(const ContextPtr & ctx);

private:
    LoggerPtr logger = getLogger("AutoStatsDaemon");
};

}
