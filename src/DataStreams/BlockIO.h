#pragma once

#include <DataStreams/IBlockStream_fwd.h>

#include <functional>

#include <Processors/QueryPipeline.h>


namespace DB
{

class ProcessListEntry;
class PlanSegmentProcessListEntry;

struct BlockIO
{
    BlockIO() = default;
    BlockIO(BlockIO &&) = default;

    BlockIO & operator= (BlockIO && rhs);
    ~BlockIO();

    BlockIO(const BlockIO &) = delete;
    BlockIO & operator= (const BlockIO & rhs) = delete;

    std::shared_ptr<ProcessListEntry> process_list_entry;
    std::shared_ptr<PlanSegmentProcessListEntry> plan_segment_process_entry;

    BlockOutputStreamPtr out;
    BlockInputStreamPtr in;

    QueryPipeline pipeline;

    /// Callbacks for query logging could be set here.
    std::function<void(IBlockInputStream *, IBlockOutputStream *, QueryPipeline *, UInt64)>    finish_callback;
    std::function<void(UInt64)>                                                                exception_callback;

    /// When it is true, don't bother sending any non-empty blocks to the out stream
    bool null_format = false;

    Stopwatch watch;

    /// Call these functions if you want to log the request.
    void onFinish()
    {
        watch.stop();
        if (finish_callback)
        {
            QueryPipeline * pipeline_ptr = nullptr;
            if (pipeline.initialized())
                pipeline_ptr = &pipeline;

            finish_callback(in.get(), out.get(), pipeline_ptr, watch.elapsedMilliseconds());
        }
    }

    void onException()
    {
        watch.stop();
        if (exception_callback)
            exception_callback(watch.elapsedMilliseconds());
    }

    /// Returns in or converts pipeline to stream. Throws if out is not empty.
    BlockInputStreamPtr getInputStream();

private:
    void reset();
};

}
