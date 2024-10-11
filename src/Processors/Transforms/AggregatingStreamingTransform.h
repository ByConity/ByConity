#pragma once
#include <Common/Logger.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Aggregator.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/IntermediateResult/OwnerInfo.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/** Aggregates streaming transform
  */
class AggregatingStreamingTransform : public ISimpleTransform
{
public:
    AggregatingStreamingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        double streaming_agg_local_ratio_,
        bool parallel_,
        bool enable_intermediate_result_cache_streaming_,
        bool cacheable_,
        bool is_parallel_merging_ = false,
        bool is_parallel_final_ = false);
    ~AggregatingStreamingTransform() override;
    String getName() const override
    {
        return "AggregatingStreamingTransform";
    }
    void work() override;
    ISimpleTransform::Status prepare() override;
    void generate(DB::Chunk & chunk);
    void processAdaptive(DB::Chunk & chunk, size_t num_rows);
    void processAdaptiveForCache(DB::Chunk & chunk, size_t num_rows);

protected:
    void transform(Chunk & chunk) override;

private:
    ALWAYS_INLINE bool checkAggregationRatio() const
    {
        return (aggregation_ratio > 0) && variants.size() / (input_rows * 1.0) < aggregation_ratio;
    }

    ALWAYS_INLINE Chunk & fetchNewChunk()
    {
        if (chunk_idx >= chunks.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "No chunk can be fetch, chunk_idx:{}, chunks_size:{}, has_left:{}, is_generated:{}, start_generated:{}, is_two_level:{}",
                chunk_idx,
                chunks.size(),
                has_left,
                is_generated,
                start_generated,
                is_two_level);
        return chunks[chunk_idx++];
    }

    //    bool continueLocalAgg(size_t chunk_rows);
    /// To read the data that was flushed into the temporary data file.
    Processors processors;

    AggregatingTransformParamsPtr params;
    LoggerPtr log = getLogger("AggregatingStreamingTransform");

    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;
    AggregateFuncStats aggregate_stats;
    AggregateFuncStats miss_aggregate_stats;

    IColumn::Filter filter;
    double aggregation_ratio;
    AggregatedDataVariants variants;
    bool is_without_key;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
     *   and if group_by_overflow_mode == ANY.
     *  In this case, new keys are not added to the set, but aggregation is performed only by
     *   keys that have already managed to get into the set.
     */
    bool no_more_keys = false;
    bool has_left = false;
    UInt64 input_rows = 0;
    UInt64 rows_returned = 0;

    Chunks chunks;
    size_t chunk_idx = 0;
    bool is_generated = false;
    bool start_generated = false;
    bool is_two_level = false;
    bool parallel;
    bool cacheable;
    bool is_parallel_merging = false;
    bool is_parallel_final = false;

    static constexpr UInt32 MIN_BLOCK_SIZE_TO_ADAPTIVE = 8192;
    OwnerInfo cur_owner_info;
};

Chunk convertToChunk(const Block & block);

}
