#pragma once

#include <unordered_map>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeBaseSelectProcessorLM.h>
#include <Common/Exception.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
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
class MergeTreeBaseSelectProcessorLM : public SourceWithProgress 
{
public:
    MergeTreeBaseSelectProcessorLM(
        Block header,
        const MergeTreeMetaBase & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const SelectQueryInfo & query_info_,
        const MergeTreeStreamSettings & stream_settings,
        const Names & virt_column_names_ = {});

    ~MergeTreeBaseSelectProcessorLM() override;

    static Block transformHeader(
        Block block, const SelectQueryInfo & query_info, const DataTypePtr & partition_value_type, const Names & virtual_columns);

private:
    bool getNewTask();
    Chunk generate() final;

protected:
    void initializeChain();
    void initializeTaskReader();
    Chunk readFromPartImpl();
    /// Can be override by each impl
    virtual bool getNewTaskImpl() = 0;
    virtual Chunk readFromPart();
    virtual void updateGranuleCounter() {}

protected:
    const MergeTreeMetaBase & storage;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeStreamSettings stream_settings;
    Names virt_column_names;
    DataTypePtr partition_value_type;
    /// header for chunks from readFromPart().
    Block header_without_virtual_columns;
    std::unique_ptr<MergeTreeReadTask> task;
    std::shared_ptr<UncompressedCache> owned_uncompressed_cache;
    std::shared_ptr<MarkCache> owned_mark_cache;
    /// Last part read in this processor
    std::string last_readed_part_name;
    /// Columns with predicate expression actions
    std::deque<AtomicPredicateExprPtr> atomic_predicate_list;
    /// Reader for each column(s)
    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
    using MergeTreeReaderPtrList = std::vector<MergeTreeReaderPtr>;
    MergeTreeReaderPtrList readers;
    /// Range reader for each column(s)
    using MergeTreeRangeReaderLMList = std::vector<MergeTreeRangeReaderLM>;
    MergeTreeRangeReaderLMList range_readers;
    /// Bitmap index readers
    using MergeTreeIndexExecutorPtr = std::shared_ptr<MergeTreeIndexExecutor>;
    using MergeTreeIndexExecutorList = std::vector<MergeTreeIndexExecutorPtr>;
    MergeTreeIndexExecutorList index_executors;
    /// Indicate the the range reader chain is ready, we need to re-init the reader chain
    /// if we change the underlying part.
    bool chain_ready = false;
    /// Indicate that we need to filter the result before returning because all predicates
    /// were pushed down
    bool need_filter = false;
    ReadBufferFromFileBase::ProfileCallback profile_callback = {};

private:
    NameSet bitmap_index_columns_super_set;

    void prepareBitMapIndexReader(const MergeTreeIndexContextPtr & index_context, const NameSet & bitmap_columns, MergeTreeIndexExecutorPtr & executor);

    void insertMarkRangesForSegmentBitMapIndexFunctions(MergeTreeIndexExecutorPtr & index_executor, const MarkRanges & mark_ranges_inc);
};

}
