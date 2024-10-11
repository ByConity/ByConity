#pragma once

#include <Interpreters/Context_fwd.h>
#include <Protos/resource_manager_rpc.pb.h>
#include <boost/noncopyable.hpp>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>

namespace DB::ResourceManagement
{

class ResourceManagerController;


class ResourceScheduler : public boost::noncopyable
{
    using ResourceRequestQueue = ConcurrentBoundedQueue<::DB::Protos::SendResourceRequestReq>;

public:
    explicit ResourceScheduler(ResourceManagerController & rm_controller_);
    ~ResourceScheduler();

    // todo (wangtao.vip): make it private
    ResourceRequestQueue queue{10000};

private:
    ContextPtr getContext() const;
    void scheduleResource();

    ResourceManagerController & rm_controller;
    LoggerPtr log;


    std::unique_ptr<ThreadPool> schedule_pool;
};

using ResourceSchedulerPtr = std::unique_ptr<ResourceScheduler>;

} // namespace DB::ResourceManagement
