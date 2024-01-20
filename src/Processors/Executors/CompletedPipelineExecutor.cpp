#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ReadProgressCallback.h>
#include <Poco/Event.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <common/scope_guard_safe.h>
#include <Common/CurrentThread.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct CompletedPipelineExecutor::Data
{
    PipelineExecutorPtr executor;
    std::exception_ptr exception;
    std::atomic_bool is_finished = false;
    std::atomic_bool has_exception = false;
    ThreadFromGlobalPool thread;
    Poco::Event finish_event;

    ~Data()
    {
        if (thread.joinable())
            thread.join();
    }
};


CompletedPipelineExecutor::CompletedPipelineExecutor(QueryPipeline & pipeline_) : pipeline(pipeline_)
{
    if (!pipeline.isCompleted())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for CompletedPipelineExecutor must be completed");
}


void CompletedPipelineExecutor::execute()
{
        PipelineExecutor executor(pipeline.getPipeProcessors(), pipeline.process_list_element);
        executor.setReadProgressCallback(pipeline.getReadProgressCallback());
        executor.execute(pipeline.getNumThreads());
}

CompletedPipelineExecutor::~CompletedPipelineExecutor()
{
    try
    {
        if (data && data->executor)
            data->executor->cancel();
    }
    catch (...)
    {
        tryLogCurrentException("PullingAsyncPipelineExecutor");
    }
}

}
