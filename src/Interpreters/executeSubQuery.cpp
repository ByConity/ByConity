#include <Core/UUID.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/executeSubQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS;
}

ContextMutablePtr createContextForSubQuery(ContextPtr context, String sub_query_tag)
{
    auto query_context = Context::createCopy(context);
    query_context->makeSessionContext();
    query_context->makeQueryContext();

    auto parent_initial_query_id = context->getInitialQueryId();
    auto uuid = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
    String initial_query_id;

    if (sub_query_tag.empty())
        initial_query_id = fmt::format("{}_{}", context->getCurrentQueryId(), uuid);
    else
        initial_query_id = fmt::format("{}_{}_{}", context->getCurrentQueryId(), sub_query_tag, uuid);

    query_context->getClientInfo().parent_initial_query_id = parent_initial_query_id;
    query_context->getClientInfo().initial_query_id = initial_query_id;
    query_context->setCurrentQueryId(initial_query_id);

    return query_context;
}

// Return a context copy with new transcation
ContextMutablePtr getContextWithNewTransaction(const ContextPtr & context, bool read_only, bool with_auth)
{
    ContextMutablePtr new_context;
    if (context->hasSessionContext())
    {
        new_context = Context::createCopy(context->getSessionContext());
    }
    else
    {
        new_context = Context::createCopy(context->getGlobalContext());
    }

    if (with_auth)
    {
        auto [user, pass] = new_context->getCnchInterserverCredentials();
        new_context->setUser(user, pass, Poco::Net::SocketAddress{});
    }
    new_context->setSettings(context->getSettings());

    if (context->tryGetCurrentWorkerGroup())
    {
        new_context->setCurrentVW(context->getCurrentVW());
        new_context->setCurrentWorkerGroup(context->getCurrentWorkerGroup());
    }

    auto txn = new_context->getCnchTransactionCoordinator().createTransaction(
        CreateTransactionOption()
            .setContext(new_context)
            .setForceCleanByDM(context->getSettingsRef().force_clean_transaction_by_dm)
            .setAsyncPostCommit(context->getSettingsRef().async_post_commit)
            .setReadOnly(read_only));
    if (txn)
    {
        new_context->setCurrentTransaction(txn);
    }
    else
    {
        throw Exception("Failed to create transaction", ErrorCodes::LOGICAL_ERROR);
    }
    return new_context;
}

void modifyQueryContext(ContextMutablePtr query_context, bool internal)
{
    query_context->setCurrentTransaction(nullptr, false);
    query_context->setCurrentVW(nullptr);
    query_context->setCurrentWorkerGroup(nullptr);
    query_context->clearCnchServerResource();

    if (internal)
        query_context->setInternalQuery(internal);
}

std::exception_ptr constructException(ContextMutablePtr query_context)
{
    auto exception_code = getCurrentExceptionCode();
    auto exception = getCurrentExceptionMessage(false);

    bool throw_root_cause = needThrowRootCauseError(query_context.get(), exception_code, exception);
    if (!throw_root_cause)
        exception = fmt::format("Query [{}] failed with : {}", query_context->getCurrentQueryId(), exception);

    return std::make_exception_ptr(Exception(std::move(exception), exception_code));
}

void executeSubQueryWithoutResult(const String & query, ContextMutablePtr query_context, bool internal)
{
    modifyQueryContext(query_context, internal);

    std::exception_ptr exception;
    auto thread = ThreadFromGlobalPool([query_context = std::move(query_context), &query, internal, &exception]() {
        try
        {
            CurrentThread::QueryScope query_scope{query_context};
            {
                ReadBufferFromOwnString in(query);
                NullWriteBuffer out;
                executeQuery(in, out, /*allow_into_outfile=*/false, query_context, /*set_result_details=*/{}, std::nullopt, internal);
            }
        }
        catch (...)
        {
            exception = constructException(query_context);
        }
    });
    thread.join();

    if (exception)
        std::rethrow_exception(exception);
}

Block executeSubQueryWithOneRow(const String & query, ContextMutablePtr query_context, bool internal, bool tolerate_multi_rows)
{
    modifyQueryContext(query_context, internal);

    Block block;
    std::exception_ptr exception;
    auto thread = ThreadFromGlobalPool([query_context = std::move(query_context), &query, internal, tolerate_multi_rows, &block, &exception]() {
        try
        {
            CurrentThread::QueryScope query_scope{query_context};
            {
                auto block_io = executeQuery(query, query_context, internal);

                auto input_stream = block_io.getInputStream();
                input_stream->readPrefix();
                block = input_stream->read();

                if (!tolerate_multi_rows && block.rows() != 1)
                    throw Exception(ErrorCodes::TOO_MANY_ROWS, "Unexcepted block");

                while (const auto & tmp_block = input_stream->read())
                {
                    if (tmp_block.rows() > 0)
                        throw Exception(ErrorCodes::TOO_MANY_ROWS, "Unexcepted block");
                }

                input_stream->readSuffix();

                block_io.onFinish();
            }
        }
        catch (...)
        {
            exception = constructException(query_context);
        }
    });
    thread.join();

    if (exception)
        std::rethrow_exception(exception);

    return block;
}

void executeSubQuery(const String & query, ContextMutablePtr query_context, std::function<void(Block &)> proc_block, bool internal)
{
    modifyQueryContext(query_context, internal);

    std::exception_ptr exception;
    auto thread = ThreadFromGlobalPool([query_context = std::move(query_context), &query, proc_block, internal, &exception]() {
        try
        {
            CurrentThread::QueryScope query_scope{query_context};
            {
                auto block_io = executeQuery(query, query_context, internal);

                auto input_stream = block_io.getInputStream();
                input_stream->readPrefix();
                while (auto block = input_stream->read())
                {
                    if (block.rows() == 0)
                        continue;
                    proc_block(block);
                }
                input_stream->readSuffix();

                block_io.onFinish();
            }
        }
        catch (...)
        {
            exception = constructException(query_context);
        }
    });
    thread.join();

    if (exception)
        std::rethrow_exception(exception);
}

Block executeSubPipelineWithOneRow(
    const ASTPtr & query, ContextMutablePtr query_context, std::function<void(InterpreterSelectQueryUseOptimizer &)> pre_execute, bool tolerate_multi_rows)
{
    if (!query->as<ASTSelectWithUnionQuery>())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unrecognized query type '{}' when executing subquery {}",
            query->getID(),
            query->formatForErrorMessage());

    Block block;
    std::exception_ptr exception;
    auto thread = ThreadFromGlobalPool([query_context = std::move(query_context), &query, pre_execute, tolerate_multi_rows, &block, &exception]() {
        try
        {
            CurrentThread::QueryScope query_scope{query_context};
            {
                SelectQueryOptions query_options;
                InterpreterSelectQueryUseOptimizer interpreter{query, query_context, query_options};
                auto block_io = interpreter.execute();
                PullingPipelineExecutor executor(block_io.pipeline);

                pre_execute(interpreter);

                while (block.rows() == 0 && executor.pull(block))
                    ;

                if (!tolerate_multi_rows && block.rows() != 1)
                    throw Exception(ErrorCodes::TOO_MANY_ROWS, "Unexcepted block");

                Block tmp_block;
                while (tmp_block.rows() == 0 && executor.pull(tmp_block))
                {
                    if (tmp_block.rows() > 0)
                        throw Exception(ErrorCodes::TOO_MANY_ROWS, "Unexcepted block");
                }
            }
        }
        catch (...)
        {
            exception = constructException(query_context);
        }
    });
    thread.join();

    if (exception)
        std::rethrow_exception(exception);

    return block;
}

void executeSubPipeline(
    const ASTPtr & query,
    ContextMutablePtr query_context,
    std::function<void(InterpreterSelectQueryUseOptimizer &)> pre_execute,
    std::function<void(Block &)> proc_block)
{
    if (!query->as<ASTSelectWithUnionQuery>())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unrecognized query type '{}' when executing subquery {}",
            query->getID(),
            query->formatForErrorMessage());

    std::exception_ptr exception;
    auto thread = ThreadFromGlobalPool([query_context = std::move(query_context), &query, pre_execute, proc_block, &exception]() {
        try
        {
            CurrentThread::QueryScope query_scope{query_context};
            {
                SelectQueryOptions query_options;
                InterpreterSelectQueryUseOptimizer interpreter{query, query_context, query_options};
                auto block_io = interpreter.execute();
                PullingPipelineExecutor executor(block_io.pipeline);

                pre_execute(interpreter);

                Block block;
                while (executor.pull(block))
                {
                    if (block.rows() == 0)
                        continue;
                    proc_block(block);
                }
            }
        }
        catch (...)
        {
            exception = constructException(query_context);
        }
    });
    thread.join();

    if (exception)
        std::rethrow_exception(exception);
}
}
