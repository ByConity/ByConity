#pragma once

#include <unordered_map>
#include <Storages/MergeTree/LateMaterialize/MergeTreeBaseSelectProcessorLM.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeRangeReaderLM.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class IMergeTreeReader;
class UncompressedCache;
class MarkCache;
struct PrewhereExprInfo;

struct AtomicPredicateExpr;

// Use in conjunction with MergeTreeReadPool to read rows from parts
// Same as MergeTreeThreadSelectProcessor, but will read from parts 
// with multi stages if possible.
class MergeTreeThreadSelectProcessorLM : public MergeTreeBaseSelectProcessorLM
{
public:
    MergeTreeThreadSelectProcessorLM(
        const size_t thread_,
        const MergeTreeReadPoolPtr & pool_,
        const MergeTreeMetaBase & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const SelectQueryInfo & query_info_,
        const MergeTreeStreamSettings & stream_settings_,
        const Names & virt_column_names_ = {});

    String getName() const override { return "MergeTreeThreadLateMaterialize"; }

    ~MergeTreeThreadSelectProcessorLM() override = default;

private:
    bool getNewTaskImpl() final;
    void updateGranuleCounter() final;

    /// "thread" index (there are N threads and each thread is assigned index in interval [0..N-1])
    size_t thread;
    std::shared_ptr<MergeTreeReadPool> pool;
    size_t min_marks_to_read;
    /// Names from header. Used in order to order columns in read blocks.
    Names ordered_names;
};

}
