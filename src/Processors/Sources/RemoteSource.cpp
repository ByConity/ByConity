/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Processors/Sources/RemoteSource.h>
#include <DataStreams/RemoteQueryExecutor.h>
#include <DataStreams/RemoteQueryExecutorReadContext.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <Interpreters/DistributedStages/PlanSegment.h>

namespace DB
{

RemoteSource::RemoteSource(RemoteQueryExecutorPtr executor, bool add_aggregation_info_, bool async_read_)
    : SourceWithProgress(executor->getHeader(), false)
    , add_aggregation_info(add_aggregation_info_), query_executor(std::move(executor))
    , async_read(async_read_)
{
    /// Add AggregatedChunkInfo if we expect DataTypeAggregateFunction as a result.
    const auto & sample = getPort().getHeader();
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            add_aggregation_info = true;
}

RemoteSource::~RemoteSource() = default;

ISource::Status RemoteSource::prepare()
{
    /// Check if query was cancelled before returning Async status. Otherwise it may lead to infinite loop.
    if (was_query_canceled)
    {
        getPort().finish();
        return Status::Finished;
    }

    if (is_async_state)
        return Status::Async;

    Status status = SourceWithProgress::prepare();
    /// To avoid resetting the connection (because of "unfinished" query) in the
    /// RemoteQueryExecutor it should be finished explicitly.
    if (status == Status::Finished)
    {
        query_executor->finish(&read_context);
        is_async_state = false;
    }
    return status;
}

std::optional<Chunk> RemoteSource::tryGenerate()
{
    /// onCancel() will do the cancel if the query was sent.
    if (was_query_canceled)
        return {};

    if (!was_query_sent)
    {
        /// Progress method will be called on Progress packet.
        query_executor->setProgressCallback([this](const Progress & value) { progress(value); });

        /// Get rows_before_limit result for remote query from ProfileInfo packet.
        query_executor->setProfileInfoCallback([this](const BlockStreamProfileInfo & info)
        {
            if (rows_before_limit && info.hasAppliedLimit())
                rows_before_limit->set(info.getRowsBeforeLimit());
        });

        query_executor->sendQuery();

        was_query_sent = true;
    }

    Block block;

    if (async_read)
    {
        auto res = query_executor->read(read_context);
        if (std::holds_alternative<int>(res))
        {
            fd = std::get<int>(res);
            is_async_state = true;
            return Chunk();
        }

        is_async_state = false;

        block = std::get<Block>(std::move(res));
    }
    else
        block = query_executor->read();

    if (!block)
    {
        query_executor->finish(&read_context);
        return {};
    }

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);

    if (add_aggregation_info)
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = block.info.bucket_num;
        info->is_overflows = block.info.is_overflows;
        chunk.setChunkInfo(std::move(info));
    }

    return chunk;
}

void RemoteSource::onCancel()
{
    was_query_canceled = true;
    query_executor->cancel(&read_context);
    // is_async_state = false;
}

void RemoteSource::onUpdatePorts()
{
    if (getPort().isFinished())
    {
        was_query_canceled = true;
        query_executor->finish(&read_context);
        // is_async_state = false;
    }
}


RemoteTotalsSource::RemoteTotalsSource(RemoteQueryExecutorPtr executor)
    : ISource(executor->getHeader())
    , query_executor(std::move(executor))
{
}

RemoteTotalsSource::~RemoteTotalsSource() = default;

Chunk RemoteTotalsSource::generate()
{
    if (auto block = query_executor->getTotals())
    {
        UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    return {};
}


RemoteExtremesSource::RemoteExtremesSource(RemoteQueryExecutorPtr executor)
    : ISource(executor->getHeader())
    , query_executor(std::move(executor))
{
}

RemoteExtremesSource::~RemoteExtremesSource() = default;

Chunk RemoteExtremesSource::generate()
{
    if (auto block = query_executor->getExtremes())
    {
        UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    return {};
}


Pipe createRemoteSourcePipe(
    RemoteQueryExecutorPtr query_executor,
    bool add_aggregation_info, bool add_totals, bool add_extremes, bool async_read, size_t resize_number_after_remote_source)
{
    Pipe pipe(std::make_shared<RemoteSource>(query_executor, add_aggregation_info, async_read));
    if (resize_number_after_remote_source > 1)
        pipe.resize(resize_number_after_remote_source);

    if (add_totals)
        pipe.addTotalsSource(std::make_shared<RemoteTotalsSource>(query_executor));

    if (add_extremes)
        pipe.addExtremesSource(std::make_shared<RemoteExtremesSource>(query_executor));

    return pipe;
}

}
