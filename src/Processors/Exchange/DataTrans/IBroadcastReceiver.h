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

#include <variant>
#include <Interpreters/QueryExchangeLog.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <butil/iobuf.h>
#include <bvar/reducer.h>
#include <Common/time.h>
#include <common/types.h>

namespace DB
{

using RecvDataPacket = std::variant<Chunk, BroadcastStatus>;
class IBroadcastReceiver
{
public:
    IBroadcastReceiver() : enable_receiver_metrics(false)
    {
    }
    explicit IBroadcastReceiver(bool enable_receiver_metrics_) : enable_receiver_metrics(enable_receiver_metrics_)
    {
    }
    struct ReceiverMetrics
    {
        bvar::Adder<size_t> recv_time_ms{};
        bvar::Adder<size_t> register_time_ms{};
        bvar::Adder<size_t> recv_rows{};
        bvar::Adder<size_t> recv_bytes{};
        bvar::Adder<size_t> recv_uncompressed_bytes{};
        bvar::Adder<size_t> recv_counts;
        bvar::Adder<size_t> dser_time_ms;
        std::atomic<Int32> finish_code{0};
        std::atomic<Int8> is_modifier{-1};
        String message;
    };
    virtual void registerToSenders(UInt32 timeout_ms) = 0;
    virtual RecvDataPacket recv(UInt32 timeout_ms)
    {
        UInt64 timeout_ms_ts = time_in_milliseconds(std::chrono::system_clock::now()) + timeout_ms;
        timespec timeout_ts {.tv_sec = long(timeout_ms_ts/1000), .tv_nsec = long(timeout_ms_ts % 1000) * 1000000};
        return recv(timeout_ts);
    }
    virtual RecvDataPacket recv(timespec timeout_ts) = 0;
    virtual BroadcastStatus finish(BroadcastStatusCode status_code, String message) = 0;
    virtual String getName() const = 0;
    virtual ~IBroadcastReceiver() = default;
    void setEnableReceiverMetrics(bool enable_) { enable_receiver_metrics = enable_; }

    bool enable_receiver_metrics = false;
    ReceiverMetrics receiver_metrics; // by default, metrics are disabled
    void addToMetricsMaybe(size_t recv_time_ms, size_t dser_time_ms, size_t recv_counts, const Chunk & chunk);
    void addToMetricsMaybe(size_t recv_time_ms, size_t dser_time_ms, size_t recv_counts, const butil::IOBuf & io_buf);
};

}
