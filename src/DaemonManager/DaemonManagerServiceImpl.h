#pragma once

#include <DaemonManager/DaemonJobServerBGThread.h>
#include <Protos/daemon_manager_rpc.pb.h>

namespace DB
{
class Context;

namespace DaemonManager
{
class DaemonManagerServiceImpl : public DB::Protos::DaemonManagerService
{
public:
    DaemonManagerServiceImpl(std::unordered_map<CnchBGThreadType, DaemonJobServerBGThreadPtr> daemon_jobs_)
        : daemon_jobs(std::move(daemon_jobs_))
    { }

    void GetAllBGThreadServers(
            ::google::protobuf::RpcController * controller,
            const ::DB::Protos::GetAllBGThreadServersReq * request,
            ::DB::Protos::GetAllBGThreadServersResp * response,
            ::google::protobuf::Closure * done) override;

    /// new API to replace GetDaemonThreadServer
    void GetDMBGJobInfo(
            ::google::protobuf::RpcController * controller,
            const ::DB::Protos::GetDMBGJobInfoReq * request,
            ::DB::Protos::GetDMBGJobInfoResp * response,
            ::google::protobuf::Closure * done) override;

    void ControlDaemonJob(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::ControlDaemonJobReq * request,
        ::DB::Protos::ControlDaemonJobResp * response,
        ::google::protobuf::Closure * done) override;

private:
    std::unordered_map<CnchBGThreadType, DaemonJobServerBGThreadPtr> daemon_jobs;
    Poco::Logger * log = &Poco::Logger::get("DaemonManagerRPCService");
};

using DaemonManagerServicePtr = std::shared_ptr<DaemonManagerServiceImpl>;

}
}
