/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <memory>
#include <tuple>
#include <Columns/IColumn.h>
#include <DataStreams/RemoteQueryExecutor.h>
#include <DataStreams/RemoteQueryExecutorReadContext.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/ExchangeBufferedSender.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/IExchangeSink.h>
#include <Processors/Exchange/RepartitionTransform.h>
#include <Processors/Exchange/SinglePartitionExchangeSink.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SinglePartitionExchangeSink::SinglePartitionExchangeSink(
    Block header_, BroadcastSenderPtr sender_, size_t partition_id_, ExchangeOptions options_, const String &name_)
    : IExchangeSink(std::move(header_))
    , name(name_)
    , header(getPort().getHeader())
    , sender(sender_)
    , partition_id(partition_id_)
    , column_num(header.columns())
    , options(options_)
    , buffered_sender(header, sender, options.send_threshold_in_bytes, options.send_threshold_in_row_num)
    , logger(getLogger("SinglePartitionExchangeSink"))
{
}

void SinglePartitionExchangeSink::consume(Chunk chunk)
{
    if (!has_input)
    {
        buffered_sender.flush(true, current_chunk_info);
        finish();
        return;
    }
    const ChunkInfoPtr & info = chunk.getChunkInfo();
    if (!info)
        throw Exception("Chunk info was not set for chunk.", ErrorCodes::LOGICAL_ERROR);
    auto repartition_info = std::dynamic_pointer_cast<const RepartitionTransform::RepartitionChunkInfo>(info);
    if (!repartition_info)
        throw Exception("Chunk should have RepartitionChunkInfo .", ErrorCodes::LOGICAL_ERROR);

    const auto & chunk_info = repartition_info->origin_chunk_info;
    bool chunk_info_matched
        = ((current_chunk_info && chunk_info && *current_chunk_info == *chunk_info) || (!current_chunk_info && !chunk_info));
    if (!chunk_info_matched)
    {
        buffered_sender.flush(true, current_chunk_info);
        current_chunk_info = chunk_info;
    }

    const IColumn::Selector & partition_selector = repartition_info->selector;

    size_t from = repartition_info->start_points[partition_id];
    size_t length = repartition_info->start_points[partition_id + 1] - from;
    if (length == 0)
        return;

    const auto & columns = chunk.getColumns();
    for (size_t i = 0; i < column_num; i++)
    {
        buffered_sender.appendSelective(i, *columns[i]->convertToFullColumnIfConst(), partition_selector, from, length);
    }
    auto status = buffered_sender.flush(false, current_chunk_info);
    if (status.code != BroadcastStatusCode::RUNNING)
        finish();
}

void SinglePartitionExchangeSink::onFinish()
{
    LOG_TRACE(logger, "SinglePartitionExchangeSink finish");
    buffered_sender.flush(true, current_chunk_info);
}

void SinglePartitionExchangeSink::onCancel()
{
    LOG_TRACE(logger, "SinglePartitionExchangeSink cancel");
    sender->finish(BroadcastStatusCode::SEND_CANCELLED, "Cancelled by pipeline");
}

}
