#include "AsyncQueryManager.h"
#include <memory>
#include <thread>

#include <Catalog/Catalog.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Protos/cnch_common.pb.h>
#include "Common/Configurations.h"
#include <Common/setThreadName.h>
#include <common/logger_useful.h>

using DB::Context;
using DB::RootConfiguration;
using DB::Catalog::Catalog;
using AsyncQueryStatus = DB::Protos::AsyncQueryStatus;

namespace DB
{
AsyncQueryManager::AsyncQueryManager(ContextWeakMutablePtr context_) : WithContext(context_)
{
    size_t max_threads = getContext()->getRootConfig().max_async_query_threads;
    pool = std::make_unique<ThreadPool>(max_threads, max_threads, max_threads, false);
}

void AsyncQueryManager::insertAndRun(std::shared_ptr<TCPQuery> info, TCPQueryHandleFunc && func)
{
    if (pool)
    {
        String id = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
        info->getContext()->setAsyncQueryId(id);
        info->sendAsyncQueryId(id);
        AsyncQueryStatus status;
        status.set_id(id);
        status.set_query_id(info->getState().query_id);
        status.set_status(AsyncQueryStatus::NotStarted);
        status.set_update_time(time(nullptr));
        info->getContext()->getCnchCatalog()->setAsyncQueryStatus(id, status);
        info->resetIOBuffer();
        pool->scheduleOrThrowOnError(
            [&, id = std::move(id), info = std::move(info), func = std::move(func), status = std::move(status)]() mutable {
                setThreadName("async_query");
                status.set_status(AsyncQueryStatus::Running);
                status.set_update_time(time(nullptr));
                info->getContext()->getCnchCatalog()->setAsyncQueryStatus(id, status);
                func(info);
            });
    }
    else
    {
        func(info);
    }
}

} // namespace DB
