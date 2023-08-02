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

void AsyncQueryManager::insertAndRun(
    BlockIO streams,
    ASTPtr ast,
    ContextMutablePtr context,
    WriteBuffer & ostr,
    const std::optional<FormatSettings> & output_format_settings,
    HttpQueryHandlerFunc && func)
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

        sendAsyncQueryId(id, context, ostr, output_format_settings);

        pool->scheduleOrThrowOnError(make_copyable_function<void()>([streams = std::move(streams),
                                                                     ast = std::move(ast),
                                                                     context = std::move(context),
                                                                     output_format_settings = std::move(output_format_settings),
                                                                     func = std::move(func),
                                                                     id = std::move(id),
                                                                     status = std::move(status)]() mutable {
            setThreadName("async_query");
            status.set_status(AsyncQueryStatus::Running);
            status.set_update_time(time(nullptr));
            context->getCnchCatalog()->setAsyncQueryStatus(id, status);

            std::optional<CurrentThread::QueryScope> query_scope;
            query_scope.emplace(context);
            func(std::move(streams), ast, context, output_format_settings);
        }));
    }
    else
    {
        func(std::move(streams), ast, context, output_format_settings);
    }
}

void AsyncQueryManager::sendAsyncQueryId(
    const String & id, ContextMutablePtr context, WriteBuffer & ostr, const std::optional<FormatSettings> & output_format_settings)
{
    MutableColumnPtr table_column_mut = ColumnString::create();
    table_column_mut->insert(id);
    Block res;
    res.insert(ColumnWithTypeAndName(std::move(table_column_mut), std::make_shared<DataTypeString>(), "async_query_id"));

    auto out = FormatFactory::instance().getOutputFormatParallelIfPossible(
        context->getDefaultFormat(), ostr, res, context, {}, output_format_settings);

    out->write(res);
    out->flush();
}

} // namespace DB
