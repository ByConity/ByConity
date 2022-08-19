#pragma once
#include <Interpreters/Context_fwd.h>
#include <DaemonManager/DaemonJob.h>
#include <DaemonManager/DaemonJobServerBGThread.h>
#include <functional>
#include <unordered_map>

namespace DB
{
class Context;

namespace DaemonManager
{
class DaemonFactory : private boost::noncopyable
{
public:
    static DaemonFactory & instance();
    using LocalDaemonJobCreator = std::function<DaemonJobPtr(ContextMutablePtr global_context)>;
    using DaemonJobForBGThreadInServerCreator = std::function<DaemonJobServerBGThreadPtr(ContextMutablePtr global_context)>;

    DaemonJobPtr createLocalDaemonJob(const String & job_name, ContextMutablePtr global_context);
    DaemonJobServerBGThreadPtr createDaemonJobForBGThreadInServer(const String & job_name, ContextMutablePtr global_context);

    template <typename T>
    void registerLocalDaemonJob(const String & job_name)
    {
        LocalDaemonJobCreator creator = [] (ContextMutablePtr global_context) { return std::make_shared<T>(std::move(global_context)); };
        if (!local_daemon_jobs.insert(std::make_pair(job_name, std::move(creator))).second)
            throw Exception("DaemonFactory: the daemon name '" + job_name + "' is not unique", ErrorCodes::LOGICAL_ERROR);
    }

    template <typename T>
    void registerDaemonJobForBGThreadInServer(const String & job_name)
    {
        DaemonJobForBGThreadInServerCreator creator = [] (ContextMutablePtr global_context) { return std::make_shared<T>(std::move(global_context)); };
        if (!daemon_jobs_for_bg_thread_in_server.insert(std::make_pair(job_name, std::move(creator))).second)
            throw Exception("DaemonFactory: the daemon name '" + job_name + "' is not unique", ErrorCodes::LOGICAL_ERROR);
    }

private:
    std::unordered_map<String, LocalDaemonJobCreator> local_daemon_jobs;
    std::unordered_map<String, DaemonJobForBGThreadInServerCreator> daemon_jobs_for_bg_thread_in_server;
};

} /// end namespace DaemonManager
} /// end namespace DB
