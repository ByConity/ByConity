#pragma once

#include <functional>
#include <memory>


namespace DB
{

class QueryPipeline;

/// Executor for completed QueryPipeline.
/// Allows to specify a callback which checks if execution should be cancelled.
/// If callback is specified, runs execution in a separate thread.
class CompletedPipelineExecutor
{
public:
    explicit CompletedPipelineExecutor(QueryPipeline & pipeline_);
    ~CompletedPipelineExecutor();

    void execute();
    struct Data;

private:
    QueryPipeline & pipeline;
    std::function<bool()> is_cancelled_callback;
    std::unique_ptr<Data> data;
};

}
