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

#include <tuple>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/MultiPartitionExchangeSink.h>
#include <Processors/Exchange/IExchangeSink.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <Columns/IColumn.h>
#include <Processors/Exchange/ExchangeBufferedSender.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/RepartitionTransform.h>

namespace DB
{
MultiPartitionExchangeSink::MultiPartitionExchangeSink(
    Block header_,
    BroadcastSenderPtrs partition_senders_,
    ExecutableFunctionPtr repartition_func_,
    ColumnNumbers repartition_keys_,
    ExchangeOptions options_,
    const String &name_)
    : IExchangeSink(std::move(header_))
    , name(name_)
    , header(getPort().getHeader())
    , partition_senders(std::move(partition_senders_))
    , partition_num(partition_senders.size())
    , column_num(header.columns())
    , repartition_func(std::move(repartition_func_))
    , repartition_keys(std::move(repartition_keys_))
    , options(options_)
    , logger(getLogger("MultiPartitionExchangeSink"))

{
    bool has_null_shuffle_key = false;
    for (size_t key_idx : repartition_keys)
    {
        const auto & type_and_name = header.safeGetByPosition(key_idx);
        if (type_and_name.type->isNullable())
        {
            has_null_shuffle_key = true;
            break;
        }
    }

    if (has_null_shuffle_key)
        repartition_result_type_ptr = &RepartitionTransform::REPARTITION_FUNC_NULLABLE_RESULT_TYPE;
    else
        repartition_result_type_ptr = &RepartitionTransform::REPARTITION_FUNC_RESULT_TYPE;

    for(size_t i = 0; i < partition_num; ++i)
    {
        ExchangeBufferedSender buffered_sender (header, partition_senders[i], options.send_threshold_in_bytes, options.send_threshold_in_row_num);
        buffered_senders.emplace_back(std::move(buffered_sender));
    }
}

void MultiPartitionExchangeSink::consume(Chunk chunk)
{

    if (partition_num == 1)
    {
        if (!has_input) {
            finish();
            return;
        }
        auto status = buffered_senders[0].sendThrough(std::move(chunk));
        if (status.code != BroadcastStatusCode::RUNNING)
            finish();
        return;
    }

    if (!has_input) {
        for(size_t i = 0; i < partition_num ; ++i)
            buffered_senders[i].flush(true, current_chunk_info);
        finish();
        return;
    }

    const auto & chunk_info = chunk.getChunkInfo();

    bool chunk_info_matched
        = ((current_chunk_info && chunk_info && *current_chunk_info == *chunk_info) || (!current_chunk_info && !chunk_info));

    if (!chunk_info_matched)
    {
        for (size_t i = 0; i < partition_num; ++i)
        {
            buffered_senders[i].flush(true, current_chunk_info);
        }
        current_chunk_info = chunk_info;
    }

    IColumn::Selector partition_selector;
    RepartitionTransform::PartitionStartPoints partition_start_points;
    std::tie(partition_selector, partition_start_points) = RepartitionTransform::doRepartition(
        partition_num, chunk, header, repartition_keys, repartition_func, *repartition_result_type_ptr);

    const auto &  columns = chunk.getColumns();
    for (size_t i = 0; i < column_num; i++)
    {
        auto materialized_column = columns[i]->convertToFullColumnIfConst();
        for (size_t j = 0; j < partition_num; ++j)
        {
            size_t from = partition_start_points[j];
            size_t length = partition_start_points[j + 1] - from;
            if (length == 0)
                continue; // no data for this partition continue;
            buffered_senders[j].appendSelective(i, *materialized_column, partition_selector, from, length);
        }
    }

    bool has_active_sender = false;
    for (size_t i = 0; i < partition_num; ++i)
    {
        auto status = buffered_senders[i].flush(false, current_chunk_info);
        if (status.code == BroadcastStatusCode::RUNNING)
            has_active_sender = true;
    }
    if (!has_active_sender)
        finish();
}

void MultiPartitionExchangeSink::onFinish()
{
    LOG_TRACE(logger, "MultiPartitionExchangeSink finish");
}

void MultiPartitionExchangeSink::onCancel()
{
    LOG_TRACE(logger, "MultiPartitionExchangeSink cancel");
    for (BroadcastSenderPtr & sender : partition_senders)
        sender->finish(BroadcastStatusCode::SEND_CANCELLED, "Cancelled by pipeline");
}

}
