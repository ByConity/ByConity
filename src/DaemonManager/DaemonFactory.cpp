#include <DaemonManager/DaemonFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
}

namespace DaemonManager
{

DaemonJobPtr DaemonFactory::createLocalDaemonJob(const String & job_key, ContextMutablePtr global_context)
{
    if (auto it = local_daemon_jobs.find(job_key); it != local_daemon_jobs.end())
    {
        return it->second(std::move(global_context));
    }

    throw Exception("Unknown type of job: " + job_key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
}

DaemonJobServerBGThreadPtr DaemonFactory::createDaemonJobForBGThreadInServer(const String & job_key, ContextMutablePtr global_context)
{
    if (auto it = daemon_jobs_for_bg_thread_in_server.find(job_key); it != daemon_jobs_for_bg_thread_in_server.end())
    {
        return it->second(std::move(global_context));
    }

    throw Exception("Unknown type of job: " + job_key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
}

DaemonFactory & DaemonFactory::instance()
{
    static DaemonFactory ret;
    return ret;
}

} /// end namespace DaemonManager
} /// end namespace DB
