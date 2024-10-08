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

#include <algorithm>
#include <atomic>
#include <optional>
#include <variant>

#include <Columns/ColumnsNumber.h>
#include <DataStreams/RemoteQueryExecutor.h>
#include <DataStreams/RemoteQueryExecutorReadContext.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Interpreters/SegmentScheduler.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/Exception.h>
#include <Common/time.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED_INTERNAL;
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
    extern const int TIMEOUT_EXCEEDED;
}

class ExchangeTotalsSource;
using ExchangeTotalsSourcePtr = std::shared_ptr<ExchangeTotalsSource>;
class ExchangeExtremesSource;
using ExchangeExtremesSourcePtr = std::shared_ptr<ExchangeExtremesSource>;

ExchangeSource::ExchangeSource(
    Block header_,
    BroadcastReceiverPtr receiver_,
    ExchangeOptions options_,
    ExchangeTotalsSourcePtr totals_source_,
    ExchangeExtremesSourcePtr extremes_source_)
    : SourceWithProgress(std::move(header_), false)
    , receiver(std::move(receiver_))
    , options(options_)
    , totals_source(std::move(totals_source_))
    , extremes_source(std::move(extremes_source_))
    , logger(getLogger("ExchangeSource"))
{
}

ExchangeSource::ExchangeSource(
    Block header_,
    BroadcastReceiverPtr receiver_,
    ExchangeOptions options_,
    bool,
    ExchangeTotalsSourcePtr totals_source_,
    ExchangeExtremesSourcePtr extremes_source_)
    : SourceWithProgress(std::move(header_), false)
    , receiver(std::move(receiver_))
    , options(options_)
    , totals_source(std::move(totals_source_))
    , extremes_source(std::move(extremes_source_))
    , logger(getLogger("ExchangeSource"))
{
}

ExchangeSource::~ExchangeSource() = default;

String ExchangeSource::getName() const
{
    return "ExchangeSource: " + receiver->getName();
}

String ExchangeSource::getClassName() const
{
    return "ExchangeSource";
}

IProcessor::Status ExchangeSource::prepare()
{
    const auto & status = SourceWithProgress::prepare();
    if (status == Status::Finished)
    {
        receiver->finish(BroadcastStatusCode::RECV_REACH_LIMIT, "ExchangeSource finished");
    }
    return status;
}

std::optional<Chunk> ExchangeSource::tryGenerate()
{
    if (was_query_canceled || was_receiver_finished)
        return std::nullopt;

    RecvDataPacket packet = receiver->recv(options.exchange_timeout_ts);

    if (std::holds_alternative<Chunk>(packet))
    {
        Chunk chunk = std::move(std::get<Chunk>(packet));
#ifndef NDEBUG
        LOG_TRACE(logger, "{} receive chunk with rows: {}", getName(), chunk.getNumRows());
#endif
        if (chunk && chunk.getChunkInfo() &&  chunk.getChunkInfo()->getType() == ChunkInfo::Type::Totals && totals_source)
        {
            totals_source->setTotals(std::move(chunk)); // assuming only one totals chunk, so it should be safe to do so.
            chunk = {};
        }
        else if (chunk && chunk.getChunkInfo() &&  chunk.getChunkInfo()->getType() == ChunkInfo::Type::Extremes && extremes_source)
        {
            extremes_source->setExtremes(std::move(chunk)); // assuming only one extremes chunk, so it should be safe to do so.
            chunk = {};
        }
        return std::make_optional(std::move(chunk));
    }
    const auto & status = std::get<BroadcastStatus>(packet);
    checkBroadcastStatus(status);
    was_receiver_finished = true;
    return std::nullopt;
}

void ExchangeSource::onCancel()
{
    LOG_TRACE(logger, "{} onCancel", getName());
    was_query_canceled = true;
    receiver->finish(BroadcastStatusCode::RECV_CANCELLED, "Cancelled by pipeline");
}

void ExchangeSource::checkBroadcastStatus(const BroadcastStatus & status) const
{
    // Better fix me. Using `>` is not a good practice to determine next move, as the `BroadcastStatusCode` may be added casually.
    if (status.code > BroadcastStatusCode::RECV_REACH_LIMIT)
    {
        if (status.is_modified_by_operator)
        {
            if(status.code == BroadcastStatusCode::RECV_TIMEOUT)
            {
                throw Exception(
                    ErrorCodes::TIMEOUT_EXCEEDED,
                    "Query {} receive data timeout, maybe you can increase settings max_execution_time. Debug info for source {}: {}",
                    CurrentThread::getQueryId(),
                    getName(),
                    status.message);
            }
            else 
            {
                // CANCELLED, NOT_READY, UNKNOWN_ERROR
                throw Exception(
                    ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION,
                    "Query {} cancels receiving data due to unknown reason with code {} and error message {}. The real error message may "
                    "be in log or query_log. Exchange source name is {}",
                    CurrentThread::getQueryId(),
                    status.code,
                    status.message,
                    getName());
            }
        }

        // If receiver is finished and not cancelly by pipeline, we should cancel pipeline here
        if (status.code != BroadcastStatusCode::RECV_CANCELLED)
            throw Exception(
                ErrorCodes::QUERY_WAS_CANCELLED_INTERNAL,
                "Query {} cancels receiving data due to unknown reason with code {} and error message {}. The real error message may "
                "be in log or query_log. Exchange source name is {}",
                CurrentThread::getQueryId(),
                status.code,
                status.message,
                getName());
    }
}

ExchangeTotalsSource::ExchangeTotalsSource(const Block& header)
    : ISource(header)
{
}

ExchangeTotalsSource::~ExchangeTotalsSource() = default;

Chunk ExchangeTotalsSource::generate()
{
    return std::move(totals);
}

void ExchangeTotalsSource::setTotals(Chunk chunk)
{
    totals = std::move(chunk);
}

ExchangeExtremesSource::ExchangeExtremesSource(const Block& header)
    : ISource(header)
{
}

ExchangeExtremesSource::~ExchangeExtremesSource() = default;

Chunk ExchangeExtremesSource::generate()
{
    return std::move(extremes);
}

void ExchangeExtremesSource::setExtremes(Chunk chunk)
{
    extremes = std::move(chunk);
}

}
