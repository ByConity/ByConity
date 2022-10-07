#include <DaemonManager/BackgroudJobExecutor.h>
#include <DaemonManager/BackgroundJob.h>
#include <CloudServices/CnchServerClient.h>

namespace DB::DaemonManager
{

namespace
{

void executeServerBGThreadAction(const BGJobInfo & job_info, CnchBGThreadAction action, const Context & context, CnchBGThreadType type)
{
    CnchServerClientPtr server_client = context.getCnchServerClient(job_info.host_port);
    server_client->controlCnchBGThread(job_info.storage_id, type, action);
    LOG_DEBUG(&Poco::Logger::get(__func__), "Succeed to {} thread for {} on {}", toString(action), job_info.storage_id.getNameForLogs(), job_info.host_port);
}

} /// end anonymous namespace

BackgroundJobExecutor::BackgroundJobExecutor(const Context & context_, CnchBGThreadType type_)
    : context{context_}, type{type_}
{}

bool BackgroundJobExecutor::start(const BGJobInfo & info)
{
    executeServerBGThreadAction(info, CnchBGThreadAction::Start, context, type);
    return true;
}

bool BackgroundJobExecutor::stop(const BGJobInfo & info)
{
    executeServerBGThreadAction(info, CnchBGThreadAction::Stop, context, type);
    return true;
}

bool BackgroundJobExecutor::remove(const BGJobInfo & info)
{
    executeServerBGThreadAction(info, CnchBGThreadAction::Remove, context, type);
    return true;
}

bool BackgroundJobExecutor::drop(const BGJobInfo & info)
{
    executeServerBGThreadAction(info, CnchBGThreadAction::Drop, context, type);
    return true;
}

bool BackgroundJobExecutor::wakeup(const BGJobInfo & info)
{
    executeServerBGThreadAction(info, CnchBGThreadAction::Wakeup, context, type);
    return true;
}

} // end namespace DB::DaemonManager
