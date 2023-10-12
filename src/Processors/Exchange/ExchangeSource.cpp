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

#include <DataStreams/RemoteQueryExecutor.h>
#include <DataStreams/RemoteQueryExecutorReadContext.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/SegmentScheduler.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
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
    , fetch_exception_from_scheduler(false)
    , totals_source(std::move(totals_source_))
    , extremes_source(std::move(extremes_source_))
    , logger(&Poco::Logger::get("ExchangeSource"))
{
}

ExchangeSource::ExchangeSource(
    Block header_,
    BroadcastReceiverPtr receiver_,
    ExchangeOptions options_,
    bool fetch_exception_from_scheduler_,
    ExchangeTotalsSourcePtr totals_source_,
    ExchangeExtremesSourcePtr extremes_source_)
    : SourceWithProgress(std::move(header_), false)
    , receiver(std::move(receiver_))
    , options(options_)
    , fetch_exception_from_scheduler(fetch_exception_from_scheduler_)
    , totals_source(std::move(totals_source_))
    , extremes_source(std::move(extremes_source_))
    , logger(&Poco::Logger::get("ExchangeSource"))
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

    RecvDataPacket packet = receiver->recv(options.exhcange_timeout_ms);

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
    was_receiver_finished = true;

    if (status.code > BroadcastStatusCode::RECV_REACH_LIMIT)
    {
        if (status.is_modifer)
            throw Exception(
                getName() + " fail to receive data: " + status.message + " code: " + std::to_string(status.code),
                ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION);
        
        // FIXME 
        // if (fetch_exception_from_scheduler)
        // {
        //     auto context = CurrentThread::get().getQueryContext();
        //     auto query_id = context->getClientInfo().initial_query_id;
        //     auto exception_with_code = context->getSegmentScheduler()->getException(query_id, options.distributed_query_wait_exception_ms);
        //     throw Exception(
        //         getName() + " fail to receive data: " + status.message + " code: " + std::to_string(status.code)
        //             + " exception: " + exception_with_code.exception, exception_with_code.code);
        // }

        // If receiver is finihsed and not cancelly by pipeline, we should cancel pipeline here
        if (status.code != BroadcastStatusCode::RECV_CANCELLED)
            throw Exception(
                getName() + " will cancel with finish message: " + status.message + " code: " + std::to_string(status.code),
                ErrorCodes::QUERY_WAS_CANCELLED);
    }

    return std::nullopt;
}

void ExchangeSource::onCancel()
{
    LOG_TRACE(logger, "ExchangeSource {} onCancel", getName());
    was_query_canceled = true;
    receiver->finish(BroadcastStatusCode::RECV_CANCELLED, "Cancelled by pipeline");
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
