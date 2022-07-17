#include <DaemonManager/DaemonManagerServiceImpl.h>
#include <DaemonManager/DaemonHelper.h>
#include <Protos/RPCHelpers.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DAEMON_MANAGER_SCHEDULE_THREAD_FAILED;
    extern const int DAEMON_THREAD_HAS_STOPPED;
}

namespace DaemonManager
{

void fillDMBGJobInfo(const BGJobInfo & bg_job_data, Protos::DMBGJobInfo & pb)
{
    RPCHelpers::fillStorageID(bg_job_data.storage_id, *pb.mutable_storage_id());
    pb.set_host_port(bg_job_data.host_port);
    pb.set_status(bg_job_data.status);
    pb.set_expected_status(bg_job_data.expected_status);
    pb.set_last_start_time(bg_job_data.last_start_time);
}

void DaemonManagerServiceImpl:: GetAllBGThreadServers(
        ::google::protobuf::RpcController *,
        const ::DB::Protos::GetAllBGThreadServersReq * request,
        ::DB::Protos::GetAllBGThreadServersResp * response,
        ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try {
        if (daemon_jobs.find(CnchBGThreadType(request->job_type())) == daemon_jobs.end())
            throw Exception(String{"No daemon job found for "}
                    + toString(CnchBGThreadType(request->job_type()))
                    + ", this may always be caused by lack of config", ErrorCodes::LOGICAL_ERROR);
        auto daemon_job = daemon_jobs[CnchBGThreadType(request->job_type())];

        auto bg_job_datas = daemon_job->getBGJobInfos();
        std::for_each(bg_job_datas.begin(), bg_job_datas.end(),
            [&response] (const BGJobInfo & bg_job_data)
            {
                fillDMBGJobInfo(bg_job_data, *response->add_dm_bg_job_infos());
            }
        );
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void DaemonManagerServiceImpl::GetDMBGJobInfo(
        ::google::protobuf::RpcController *,
        const ::DB::Protos::GetDMBGJobInfoReq * request,
        ::DB::Protos::GetDMBGJobInfoResp * response,
        ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        UUID storage_uuid = RPCHelpers::createUUID(request->storage_uuid());
        auto it = daemon_jobs.find(CnchBGThreadType(request->job_type()));
        if (it == daemon_jobs.end())
            throw Exception(String{"No daemon job found for "}
                    + toString(CnchBGThreadType(request->job_type()))
                    + ", this may always be caused by lack of config", ErrorCodes::LOGICAL_ERROR);
        auto daemon_job = it->second;
        BackgroundJobPtr bg_job_ptr = daemon_job->getBackgroundJob(storage_uuid);
        if (!bg_job_ptr)
        {
            LOG_INFO(log, "No background job found for uuid: {}", toString(storage_uuid));
            return;
        }
        BGJobInfo bg_job_data = bg_job_ptr->getBGJobInfo();
        fillDMBGJobInfo(bg_job_data, *response->mutable_dm_bg_job_info());
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

static String getDaemonActionName(const ::DB::Protos::ControlDaemonJobReq_Action & action)
{
    switch (action)
    {
        case ::DB::Protos::ControlDaemonJobReq::Stop:
            return "Stop";
        case ::DB::Protos::ControlDaemonJobReq::Start:
            return "Start";
        case ::DB::Protos::ControlDaemonJobReq::Remove:
            return "Remove";
        case ::DB::Protos::ControlDaemonJobReq::Drop:
            return "Drop";
    }
}

void DaemonManagerServiceImpl::ControlDaemonJob(
    ::google::protobuf::RpcController *,
    const ::DB::Protos::ControlDaemonJobReq * request,
    ::DB::Protos::ControlDaemonJobResp * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        StorageID storage_id = RPCHelpers::createStorageID(request->storage_id());

        LOG_INFO(log, "Receive ControlDaemonJob RPC request for storage: {} job type: {} action: {}"
            , storage_id.getNameForLogs()
            , toString(CnchBGThreadType(request->job_type()))
            , getDaemonActionName(request->action()));

        if (daemon_jobs.find(CnchBGThreadType(request->job_type())) == daemon_jobs.end())
            throw Exception(String{"No daemon job found for "}
                    + toString(CnchBGThreadType(request->job_type()))
                    + ", this may always be caused by lack of config", ErrorCodes::LOGICAL_ERROR);
        auto daemon_job = daemon_jobs[CnchBGThreadType(request->job_type())];

        switch (request->action())
        {
            case Protos::ControlDaemonJobReq::Stop:
            {
                Result res = daemon_job->executeJobAction(storage_id, CnchBGThreadAction::Stop);
                if (!res.res)
                    throw Exception(res.error_str, ErrorCodes::LOGICAL_ERROR);
                break;
            }
            case Protos::ControlDaemonJobReq::Start:
            {
                Result res = daemon_job->executeJobAction(storage_id, CnchBGThreadAction::Start);
                if (!res.res)
                    throw Exception(res.error_str, ErrorCodes::LOGICAL_ERROR);
                break;
            }
            case Protos::ControlDaemonJobReq::Remove:
            {
                Result res = daemon_job->executeJobAction(storage_id, CnchBGThreadAction::Remove);
                if (!res.res)
                    throw Exception(res.error_str, ErrorCodes::LOGICAL_ERROR);
                break;
            }
            case Protos::ControlDaemonJobReq::Drop:
            {
                Result res = daemon_job->executeJobAction(storage_id, CnchBGThreadAction::Drop);
                if (!res.res)
                    throw Exception(res.error_str, ErrorCodes::LOGICAL_ERROR);
                break;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}
}
}
