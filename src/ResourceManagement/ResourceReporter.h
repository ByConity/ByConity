#pragma once
#include <Core/BackgroundSchedulePool.h>
#include <Common/ResourceMonitor.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class Context;

namespace ResourceManagement
{

class ResourceReporterTask : protected WithContext
{
public:
    ResourceReporterTask(ContextPtr context);
    ~ResourceReporterTask();
    void run();
    void start();
    void stop();

private:
    bool sendHeartbeat();
    void sendRegister();
    void sendRemove();

    inline String getenv(const char * name) { return std::getenv(name) ? std::getenv(name) : ""; }

private:
    bool init_request = true;

    Poco::Logger * log;
    std::unique_ptr<ResourceMonitor> resource_monitor;
    BackgroundSchedulePool::TaskHolder background_task;
};

}

}
