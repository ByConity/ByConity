#include "AsyncQueryManager.h"
#include <memory>
#include <optional>
#include <thread>

#include <Catalog/Catalog.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Protos/cnch_common.pb.h>
#include "Common/Configurations.h"
#include <Common/setThreadName.h>
#include <common/logger_useful.h>
#include "Formats/FormatSettings.h"
#include "IO/WriteBuffer.h"

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

void AsyncQueryManager::insertAndRun(
    BlockIO streams, ASTPtr ast, ContextMutablePtr context, SendAsyncQueryIdCallback send_async_query_id, AsyncQueryHandlerFunc && func)
{
    if (pool)
    {
        String id = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
        context->setAsyncQueryId(id);
        AsyncQueryStatus status;
        status.set_id(id);
        status.set_query_id(context->getClientInfo().current_query_id);
        status.set_status(AsyncQueryStatus::NotStarted);
        status.set_update_time(time(nullptr));
        context->getCnchCatalog()->setAsyncQueryStatus(id, status);

        send_async_query_id(id);

        pool->scheduleOrThrowOnError(make_copyable_function<void()>([streams = std::move(streams),
                                                                     ast = std::move(ast),
                                                                     context = std::move(context),
                                                                     func = std::move(func),
                                                                     id = std::move(id),
                                                                     status = std::move(status)]() mutable {
            setThreadName("async_query");
            status.set_status(AsyncQueryStatus::Running);
            status.set_update_time(time(nullptr));
            context->getCnchCatalog()->setAsyncQueryStatus(id, status);

            std::optional<CurrentThread::QueryScope> query_scope;
            query_scope.emplace(context);
            func(std::move(streams), ast, context);
        }));
    }
    else
    {
        func(std::move(streams), ast, context);
    }
}

} // namespace DB
