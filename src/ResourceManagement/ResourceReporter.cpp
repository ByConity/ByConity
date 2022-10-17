#include <ResourceManagement/ResourceReporter.h>

#include <Interpreters/Context.h>
#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/ResourceManagerClient.h>



namespace DB::ErrorCodes
{
    extern const int NO_SUCH_SERVICE;
}

namespace DB::ResourceManagement
{

ResourceReporterTask::ResourceReporterTask(ContextPtr global_context_)
    : WithContext(global_context_)
    , log(&Poco::Logger::get("ResourceReporterTask"))
    , resource_monitor(std::make_unique<ResourceMonitor>(global_context_))
    , background_task(global_context_->getSchedulePool().createTask("ResourceReporterTask", [&](){ run(); }))
{
    LOG_TRACE(log, "Create ResourceReporterTask.");
    background_task->activateAndSchedule();
}

ResourceReporterTask::~ResourceReporterTask()
{
    try
    {
        LOG_TRACE(log, "Remove ResourceReporterTask.");
        background_task->deactivate();
        sendRemove();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void ResourceReporterTask::run()
{
    try
    {
        if (init_request)
        {
            sendRegister();
            init_request = false;
        }
        else
        {
            if (!sendHeartbeat())
            {
                init_request = true; // Ensure register eventually succeeds
                sendRegister();
            }
        }
        //TODO: Change to config setting
        background_task->scheduleAfter(1000);
    }
    catch (Exception & e)
    {
        // FIXME
        if (e.code() != ErrorCodes::NO_SUCH_SERVICE)
            background_task->scheduleAfter(3000);

        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

bool ResourceReporterTask::sendHeartbeat()
{
    auto resource_manager = getContext()->getResourceManagerClient();
    auto data = resource_monitor->createResourceData();

    LOG_TRACE(log, "Send heartbeat to RM: {} self: {}", resource_manager->leader_host_port, data.host_ports.toDebugString());
    data.id = getenv("WORKER_ID");
    data.vw_name = getenv("VIRTUAL_WAREHOUSE_ID");
    data.worker_group_id = getenv("WORKER_GROUP_ID");
    return resource_manager->reportResourceUsage(data);
}

void ResourceReporterTask::sendRegister()
{
    auto resource_manager = getContext()->getResourceManagerClient();
    auto data = resource_monitor->createResourceData(true);

    LOG_TRACE(log, "Register Node in RM: {} self: {}", resource_manager->leader_host_port, data.host_ports.toDebugString());
    data.id = getenv("WORKER_ID");
    data.vw_name = getenv("VIRTUAL_WAREHOUSE_ID");
    data.worker_group_id = getenv("WORKER_GROUP_ID");
    resource_manager->registerWorker(data);
}

void ResourceReporterTask::sendRemove()
{
    auto resource_manager = getContext()->getResourceManagerClient();
    try
    {
        resource_manager->removeWorker(getenv("WORKER_ID"), getenv("VIRTUAL_WAREHOUSE_ID"), getenv("WORKER_GROUP_ID"));
    }
    catch (...)
    {
        tryLogCurrentException("ResourceReporter::sendRemove", "Failed to unregister from RM " + resource_manager->leader_host_port);
    }
}

void ResourceReporterTask::start()
{
    LOG_TRACE(log, "Start ResourceReporterTask.");
    background_task->activateAndSchedule();
}

void ResourceReporterTask::stop()
{
    LOG_TRACE(log, "Stop ResourceReporterTask.");
    background_task->deactivate();
    sendRemove();
}

}
