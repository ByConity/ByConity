#pragma once

#include <DaemonManager/DaemonHelper.h>
#include <DaemonManager/BackgroundJob.h>
#include <Interpreters/StorageID.h>
#include <CloudServices/RpcClientBase.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <DaemonManager/DaemonManagerClient_fwd.h>

namespace DB
{
namespace Protos
{
    class DaemonManagerService_Stub;
}

namespace DaemonManager
{
class DaemonManagerClient : public RpcClientBase
{
public:
    static String getName() { return "DaemonManagerClient"; }

    DaemonManagerClient(String host_port);
    DaemonManagerClient(HostWithPorts host_ports);

    ~DaemonManagerClient() override;

    BGJobInfos getAllBGThreadServers(CnchBGThreadType type);
    std::optional<BGJobInfo> getDMBGJobInfo(const UUID & storage_uuid, CnchBGThreadType type);
    void controlDaemonJob(const StorageID & storage_id, CnchBGThreadType job_type, CnchBGThreadAction action);
    void forwardOptimizeQuery(const StorageID & storage_id, const String & partition_id, bool enable_try);

private:
    std::unique_ptr<Protos::DaemonManagerService_Stub> stub_ptr;
};

}
}
