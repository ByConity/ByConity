#include "AsyncQueryManager.h"
#include <memory>
#include <optional>
#include <thread>
#include <ucontext.h>
#include <Catalog/Catalog.h>
#include <Core/UUID.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <MergeTreeCommon/CnchServerLeader.h>
#include <Protos/cnch_common.pb.h>
#include <Common/Configurations.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>
#include <common/types.h>

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

AsyncQueryManager::~AsyncQueryManager()
{
    if (pool)
    {
        pool->wait();
    }
}

void AsyncQueryManager::insertAndRun(
    String & query,
    ASTPtr ast,
    ContextMutablePtr ctx,
    ReadBuffer * istr,
    SendAsyncQueryIdCallback send_async_query_id,
    AsyncQueryHandlerFunc && func)
{
    if (pool)
    {
        String id = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
        ctx->setAsyncQueryId(id);
        AsyncQueryStatus status;
        status.set_id(id);
        status.set_query_id(ctx->getClientInfo().current_query_id);
        status.set_status(AsyncQueryStatus::NotStarted);
        auto c_time = time(nullptr);
        status.set_start_time(c_time);
        status.set_update_time(c_time);
        status.set_max_execution_time(ctx->getSettingsRef().max_execution_time.totalSeconds());
        ctx->getCnchCatalog()->setAsyncQueryStatus(id, status);

        send_async_query_id(id);

        pool->scheduleOrThrowOnError(make_copyable_function<void()>([query = std::move(query),
                                                                     ast = std::move(ast),
                                                                     context = std::move(ctx),
                                                                     istr,
                                                                     func = std::move(func),
                                                                     id = std::move(id),
                                                                     status = std::move(status)]() mutable {
            setThreadName("async_query");
            status.set_status(AsyncQueryStatus::Running);
            status.set_update_time(time(nullptr));
            context->getCnchCatalog()->setAsyncQueryStatus(id, status);

            std::optional<CurrentThread::QueryScope> query_scope;
            query_scope.emplace(context);
            func(query, ast, context, istr);
        }));
    }
    else
    {
        func(query, ast, ctx, istr);
    }
}

} // namespace DB
