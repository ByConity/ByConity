#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeSelectProcessorLM.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{


/// Used to read data from single part with select query
/// Cares about PREWHERE, virtual columns, indexes etc.
/// To read data from multiple parts, Storage (MergeTree) creates multiple such objects.
class MergeTreeReverseSelectProcessorLM : public MergeTreeSelectProcessorLM
{
public:
    template<typename... Args>
    explicit MergeTreeReverseSelectProcessorLM(Args &&... args)
        : MergeTreeSelectProcessorLM{std::forward<Args>(args)...},
        is_first_task(true)
    {
        LOG_TRACE(log, "Reading {} ranges in reverse order from part {}, approx. {} rows starting from {}",
            all_mark_ranges.size(), data_part->name, total_rows,
            data_part->index_granularity.getMarkStartingRow(all_mark_ranges.front().begin));
    }

    ~MergeTreeReverseSelectProcessorLM() override = default;

    String getName() const override { return "MergeTreeReverseLateMaterialize"; }

protected:

    bool getNewTaskImpl() override;
    Chunk readFromPart() override;

private:
    bool is_first_task;
    Chunks chunks;
    static inline Poco::Logger * log = &Poco::Logger::get("MergeTreeReverseSelectProcessor");
};

}
