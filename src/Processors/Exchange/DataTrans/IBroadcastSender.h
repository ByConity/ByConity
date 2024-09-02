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

#pragma once

#include <optional>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <bvar/reducer.h>
#include <Common/Stopwatch.h>
#include <common/types.h>
#include <Interpreters/QueryExchangeLog.h>

namespace DB
{


enum class BroadcastSenderType
{
    Local = 0,
    Brpc = 1,
    Disk = 2
};
class IBroadcastSender
{
public:
    IBroadcastSender() : enable_sender_metrics(false)
    {
    }
    explicit IBroadcastSender(bool enable_sender_metrics_) : enable_sender_metrics(enable_sender_metrics_)
    {
    }
    /// metrics for sender, bvar is used for write intensive case
    struct SenderMetrics
    {
        /// all senders
        bvar::Adder<size_t> send_time_ms{};
        bvar::Adder<size_t> send_rows{};
        bvar::Adder<size_t> send_uncompressed_bytes{};
        bvar::Adder<size_t> num_send_times{};
        /// for brpc senders
        bvar::Adder<size_t> send_bytes{};
        bvar::Adder<size_t> ser_time_ms{};
        bvar::Adder<size_t> send_retry{};
        bvar::Adder<size_t> send_retry_ms{};
        bvar::Adder<size_t> overcrowded_retry{};
        /// finish related
        std::atomic<Int32> finish_code{};
        std::atomic<Int8> is_modifier{-1};
        String message;
    };
    BroadcastStatus send(Chunk chunk) noexcept;
    /// sendImpl should be thread-safe
    virtual BroadcastStatus sendImpl(Chunk chunk) = 0;
    /// finish should be thread-safe
    virtual BroadcastStatus finish(BroadcastStatusCode status_code, String message) = 0;
    /// Merge sender to get 1:N sender and can avoid duplicated serialization when send chunk
    virtual void merge(IBroadcastSender && sender) = 0;

    virtual String getName() const = 0;
    virtual BroadcastSenderType getType() = 0;
    virtual bool needMetrics() { return true; }
    virtual ~IBroadcastSender() = default;
    SenderMetrics & getSenderMetrics()
    {
        return sender_metrics;
    }

protected:
    bool enable_sender_metrics = false; // by default, metrics are disabled
    SenderMetrics sender_metrics;

private:
    Stopwatch sender_timer;
};

using BroadcastSenderPtr = std::shared_ptr<IBroadcastSender>;

}
