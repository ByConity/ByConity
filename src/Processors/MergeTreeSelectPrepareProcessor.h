#pragma once

#include <Processors/ISource.h>
#include <QueryPlan/TableScanStep.h>


namespace DB
{

class MergeTreeSelectPrepareProcessor : public ISource
{
public:
    MergeTreeSelectPrepareProcessor(
        TableScanStep & step_,
        const BuildQueryPipelineSettings & build_settings,
        Block header,
        const std::vector<RuntimeFilterId> & ids_,
        UInt64 wait_time);

    String getName() const override
    {
        return "MergeTreeSelectPrepareProcessor";
    }

    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

private:
    TableScanStep & step;
    BuildQueryPipelineSettings settings;
    Processors processors;
    UInt64 rf_wait_time_ns;
    std::vector<std::string> runtime_filters;
    Stopwatch timing;
    bool poll_done = false;
    bool start_poll = false;
    bool start_expand = false;
};

}
