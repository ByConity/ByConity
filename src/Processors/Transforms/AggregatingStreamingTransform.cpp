#include <Processors/Transforms/AggregatingStreamingTransform.h>

#include <Columns/ColumnsCommon.h>
#include <DataStreams/materializeBlock.h>
#include <Processors/ISource.h>
#include <Processors/Pipe.h>

namespace ProfileEvents
{
extern const Event ExternalAggregationMerge;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int LOGICAL_ERROR;
}


AggregatingStreamingTransform::AggregatingStreamingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    double aggregation_ratio_,
    bool parallel_,
    bool enable_intermediate_result_cache_streaming_,
    bool cacheable_,
    bool is_parallel_merging_,
    bool is_parallel_final_)
    : ISimpleTransform({std::move(header)}, {params_->getHeader()}, true)
    , params(std::move(params_))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
    , aggregation_ratio(aggregation_ratio_)
    , parallel(parallel_)
    , cacheable(cacheable_)
    , is_parallel_merging(is_parallel_merging_)
    , is_parallel_final(is_parallel_final_)
{
    if (!is_parallel_final && enable_intermediate_result_cache_streaming_)
        params->aggregator.turnOnAggStreaming();

    if (!enable_intermediate_result_cache_streaming_)
        params->aggregator.turnOnAggConvertingForCache();

    is_without_key = params->aggregator.isWithoutKey();
    if (variants.empty())
    {
        variants.init(params->aggregator.method_chosen);
        variants.keys_size = params->params.keys_size;
        variants.key_sizes = params->aggregator.key_sizes;
    }
    is_two_level = variants.isConvertibleToTwoLevel();
}

AggregatingStreamingTransform::~AggregatingStreamingTransform() = default;

ISimpleTransform::Status AggregatingStreamingTransform::prepare()
{
    /// Output if has data.
    if (has_output)
    {
        output.pushData(std::move(output_data));
        has_output = false;
        return Status::PortFull;
    }

    if (has_left && start_generated)
    {
        output_data.chunk = std::move(fetchNewChunk());
        output.pushData(std::move(output_data));
        has_left = chunk_idx != chunks.size();
        return Status::PortFull;
    }

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Stop if don't need more data.
    if (no_more_data_needed)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    /// Check can input.
    if (!has_input)
    {
        if (input.isFinished())
        {
            if (has_left)
            {
                start_generated = true;
                return Status::Ready;
            }
            if (!start_generated)
            {
                /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
                /// To do this, we pass a block with zero rows to aggregate.
                if (params->params.keys_size == 0 && !params->params.empty_result_for_aggregation_by_empty_set)
                {
                    params->aggregator.executeOnBlock(
                        getInputs().front().getHeader(), variants, key_columns, aggregate_columns, no_more_keys);
                    has_left = true;
                    start_generated = true;
                    return Status::Ready;
                }
            }
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        input_data = input.pullData(set_input_not_needed_after_read);
        has_input = true;

        if (input_data.exception)
            /// No more data needed. Exception will be thrown (or swallowed) later.
            input.setNotNeeded();
    }

    /// Now transform.
    return Status::Ready;
}
void AggregatingStreamingTransform::work()
{
    has_input = false;
    if (!input_data.chunk.hasRows() && !has_left)
    {
        has_output = false;
        return;
    }

    transform(input_data.chunk);
    output_data.chunk.swap(input_data.chunk);

    if (output_data.chunk.hasRows())
        has_output = true;
}

void AggregatingStreamingTransform::transform(DB::Chunk & chunk)
{
    UInt64 num_rows = chunk.getNumRows();
    if (num_rows == 0)
    {
        if (has_left && start_generated)
        {
            if (!is_generated)
            {
                generate(chunk);
                if (!is_without_key)
                    is_generated = true;
                return;
            }

            chunk = std::move(fetchNewChunk());
            has_left = chunk_idx != chunks.size();
        }
        return;
    }

    if (is_parallel_final)
    {
        params->aggregator.executeOnBlock(chunk.detachColumns(), num_rows, variants, key_columns, aggregate_columns, no_more_keys);
        input_rows += num_rows;
        has_left = true;
        return;
    }

    if (is_parallel_merging)
    {
        input_rows += num_rows;
        auto block = inputs.front().getHeader().cloneWithColumns(chunk.getColumns());
        if (!params->aggregator.mergeOnBlock(block, variants, no_more_keys, is_cancelled))
        {
            start_generated = true;
            no_more_data_needed = true;
        }

        if (variants.isConvertibleToTwoLevel() && variants.size() > 1)
            variants.convertToTwoLevel();


        has_left = true;
        return;
    }

    if (cacheable)
        processAdaptiveForCache(chunk, num_rows);
    else
        processAdaptive(chunk, num_rows);
}

void AggregatingStreamingTransform::processAdaptive(DB::Chunk & chunk, size_t num_rows)
{
    if (input_rows == 0 || input_rows < MIN_BLOCK_SIZE_TO_ADAPTIVE)
    {
        params->aggregator.executeOnBlock(chunk.detachColumns(), num_rows, variants, key_columns, aggregate_columns, no_more_keys);
        input_rows += num_rows;
        has_left = true;
        return;
    }

    if (is_without_key || checkAggregationRatio())
    {
        params->aggregator.executeOnBlock(chunk.detachColumns(), num_rows, variants, key_columns, aggregate_columns, no_more_keys);
        has_left = true;
        input_rows += num_rows;
        LOG_TRACE(log, "continue local merge:{}", num_rows);
        return;
    }

    params->aggregator.executeOnBlock(chunk.detachColumns(), num_rows, variants, key_columns, aggregate_columns, no_more_keys);
    LOG_TRACE(log, "direct output local merge:{}", num_rows);
    Block block = params->aggregator.prepareBlockAndFillSingleLevel(variants, params->final);
    block.info.bucket_num = is_two_level ? 0 : -1;
    chunk = convertToChunk(block);
    input_rows = 0;
}

void AggregatingStreamingTransform::processAdaptiveForCache(DB::Chunk & chunk, size_t num_rows)
{
    if (cur_owner_info.empty())
        cur_owner_info = chunk.getOwnerInfo();

    if (cur_owner_info == chunk.getOwnerInfo() && (!parallel || checkAggregationRatio()))
    {
        params->aggregator.executeOnBlock(chunk.detachColumns(), num_rows, variants, key_columns, aggregate_columns, no_more_keys);
        has_left = true;
        return;
    }

    auto save_chunk = std::move(chunk);

    generate(chunk);

    cur_owner_info = save_chunk.getOwnerInfo();
    params->aggregator.executeOnBlock(save_chunk.detachColumns(), num_rows, variants, key_columns, aggregate_columns, no_more_keys);
    has_left = true;
}

void AggregatingStreamingTransform::generate(DB::Chunk & chunk)
{
    auto blocks_list = params->aggregator.convertToBlocks(variants, params->final, 1);
    for (auto & block : blocks_list)
    {
        if (!cacheable)
            block.info.bucket_num = is_two_level ? 0 : -1;
        auto tmp_chunk = convertToChunk(block);
        tmp_chunk.setOwnerInfo(cur_owner_info);
        chunks.emplace_back(std::move(tmp_chunk));
    }

    chunk = std::move(fetchNewChunk());
    rows_returned += chunk.getNumRows();
    has_left = chunk_idx != chunks.size();
    LOG_TRACE(
        log, "{} blocks generate, {} chunks remain, {} rows return", blocks_list.size(), chunks.size() - chunk_idx, chunk.getNumRows());
}
}
