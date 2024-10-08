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

#include <Common/Logger.h>
#include <Core/Block.h>
#include <Interpreters/QueryExchangeLog.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <Processors/Exchange/DataTrans/Brpc/AsyncRegisterResult.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastSender.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/MultiPathBoundedQueue.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <brpc/stream.h>
#include <Poco/Logger.h>

#include <atomic>
#include <optional>
#include <vector>

namespace DB
{
class BrpcRemoteBroadcastReceiver : public std::enable_shared_from_this<BrpcRemoteBroadcastReceiver>, public IBroadcastReceiver
{
public:
    BrpcRemoteBroadcastReceiver(
        ExchangeDataKeyPtr trans_key_,
        String registry_address_,
        ContextPtr context_,
        Block header_,
        bool keep_order_,
        const String &name_,
        BrpcExchangeReceiverRegistryService::RegisterMode mode_ = BrpcExchangeReceiverRegistryService::RegisterMode::BRPC);

    BrpcRemoteBroadcastReceiver(
        ExchangeDataKeyPtr trans_key_,
        String registry_address_,
        ContextPtr context_,
        Block header_,
        bool keep_order_,
        const String & name_,
        MultiPathQueuePtr queue_,
        BrpcExchangeReceiverRegistryService::RegisterMode mode_ = BrpcExchangeReceiverRegistryService::RegisterMode::BRPC,
        std::shared_ptr<QueryExchangeLog> query_exchange_log_ = nullptr,
        String coordinator_address_ = "");

    ~BrpcRemoteBroadcastReceiver() override;

    void registerToSenders(UInt32 timeout_ms) override;
    RecvDataPacket recv(timespec timeout_ms) override;
    BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;
    String getName() const override;
    void pushReceiveQueue(MultiPathDataPacket packet);
    void setSendDoneFlag() { send_done_flag.test_and_set(std::memory_order_release); }

    static String
    generateName(size_t exchange_id, size_t write_segment_id, size_t read_segment_id, size_t parallel_index, const String & co_host_port)
    {
        return fmt::format(
            "BrpcReciver[{}_{}_{}_{}_{}]",
            write_segment_id,
            read_segment_id,
            parallel_index,
            exchange_id,
            co_host_port
        );
    }

    static String generateNameForTest()
    {
        return fmt::format(
            "BrpcReciver[{}_{}_{}_{}_{}]",
            "test-BrpcReciver", -1, -1, -1, -1
        );
    }

    AsyncRegisterResult registerToSendersAsync(UInt32 timeout_ms);
private:
    String name;
    LoggerPtr log = getLogger("BrpcRemoteBroadcastReceiver");
    ExchangeDataKeyPtr trans_key;
    String registry_address;
    ContextPtr context;
    Block header;
    std::atomic<BroadcastStatusCode> finish_status_code{BroadcastStatusCode::RUNNING};
    std::atomic_flag send_done_flag = ATOMIC_FLAG_INIT;
    MultiPathQueuePtr queue;
    brpc::StreamId stream_id{brpc::INVALID_STREAM_ID};
    bool keep_order;
    String initial_query_id;
    BrpcExchangeReceiverRegistryService::RegisterMode mode;
    std::shared_ptr<QueryExchangeLog> query_exchange_log;
    String coordinator_address;

    void sendRegisterRPC(
        Protos::RegistryService_Stub & stub,
        brpc::Controller & cntl,
        Protos::RegistryRequest * request,
        Protos::RegistryResponse * response,
        google::protobuf::Closure * done);
};

using BrpcRemoteBroadcastReceiverShardPtr = std::shared_ptr<BrpcRemoteBroadcastReceiver>;
using BrpcRemoteBroadcastReceiverWeakPtr = std::weak_ptr<BrpcRemoteBroadcastReceiver>;
using BrpcReceiverPtrs = std::vector<BrpcRemoteBroadcastReceiverShardPtr>;
}
