#pragma once

#include <DaemonManager/DaemonHelper.h>
#include <DaemonManager/BackgroundJob.h>
#include <Interpreters/StorageID.h>
#include <Protos/daemon_manager_rpc.pb.h>
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

    BGJobInfos getAllBGThreadServers(CnchBGThreadType type);

    /// new API to replace getDaemonThreadServer
    std::optional<BGJobInfo> getDMBGJobInfo(const UUID & storage_uuid, CnchBGThreadType type);

    String getDaemonThreadServer(const StorageID & storage_id, CnchBGThreadType type);

    void controlDaemonJob(const StorageID & storage_id, CnchBGThreadType job_type, Protos::ControlDaemonJobReq::Action action);

private:
    std::unique_ptr<Protos::DaemonManagerService_Stub> stub_ptr;
};

}
}
