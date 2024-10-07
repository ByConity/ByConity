#include <Processors/MergeTreeSelectPrepareProcessor.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ResizeProcessor.h>

#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeSelectPrepareProcessor::MergeTreeSelectPrepareProcessor(
    TableScanStep & step_,
    const BuildQueryPipelineSettings & build_settings,
    Block header,
    const std::vector<RuntimeFilterId> & ids_,
    UInt64 wait_time)
    : ISource(std::move(header)), step(step_), settings(build_settings), rf_wait_time_ns(wait_time * 1000000UL)
{
    for (const auto id : ids_)
        runtime_filters.emplace_back(RuntimeFilterManager::makeKey(build_settings.distributed_settings.query_id, id));
}

IProcessor::Status MergeTreeSelectPrepareProcessor::prepare()
{
    // LOG_ERROR(getLogger("MergeTreeSelectPrepareProcessor"), "thread:{}", current_thread->thread_id);
    if (inputs.empty())
    {
        if (start_expand)
            return Status::ExpandPipeline;

        if (!start_poll)
        {
            timing.restart();
            start_poll = true;
        }

        return Status::Ready;
    }

    auto output = outputs.begin();
    auto input = inputs.begin();
    if (output->isFinished())
    {
        input->close();
        finished = true;
        return Status::Finished;
    }

    if (!output->isNeeded())
        return Status::PortFull;

    if (input->isFinished())
    {
        output->finish();
        finished = true;
        return Status::Finished;
    }

    input->setNeeded();
    if (!input->hasData())
        return Status::NeedData;

    output->pushData(input->pullData(true));
    return Status::PortFull;
}

void MergeTreeSelectPrepareProcessor::work()
{
    if (!poll_done)
    {
        if (timing.elapsed() > rf_wait_time_ns)
        {
            poll_done = true;
            for (const auto & rf : runtime_filters)
            {
                if (!RuntimeFilterManager::getInstance().getDynamicValue(rf)->isReady())
                {
                     LOG_DEBUG(
                        getLogger("MergeTreeSelectPrepareProcessor"),
                        "wait time out:{} rf:{}",
                        timing.elapsed(),
                        rf);
                }
            }
        }
        else
        {
            bool all_ready = true;
            for (const auto & rf : runtime_filters)
                all_ready = all_ready && RuntimeFilterManager::getInstance().getDynamicValue(rf)->isReady();

            if (all_ready)
            {
                //                LOG_DEBUG(getLogger("MergeTreeSelectPrepareProcessor"), "cost time:{} thread:{}", timing.elapsed(),
                //                          current_thread->thread_id);
                poll_done = true;
            }
        }

        if (poll_done)
            start_expand = true;
        return;
    }

    if (!start_expand)
        start_expand = true;
}

Processors MergeTreeSelectPrepareProcessor::expandPipeline()
{
    QueryPipeline pipeline;
    settings.is_expand = true;
    step.initializePipeline(pipeline, settings);

    pipeline.addTransform(std::make_shared<ResizeProcessor>(outputs.front().getHeader(), pipeline.getNumStreams(), 1));
    processors.insert(processors.end(), pipeline.getProcessors().begin(), pipeline.getProcessors().end());

    inputs.emplace_back(outputs.front().getHeader(), this);
    connect(processors.back()->getOutputs().front(), inputs.back());
    return processors;
}

}
