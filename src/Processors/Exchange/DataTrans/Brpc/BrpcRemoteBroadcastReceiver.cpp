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

#include <atomic>
#include <memory>
#include <brpc/stream.h>

#include <IO/WriteBufferFromString.h>
#include <Interpreters/QueryExchangeLog.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/Brpc/StreamHandler.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/MultiPathBoundedQueue.h>
#include <Processors/Exchange/DataTrans/NativeChunkOutputStream.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Processors/Exchange/DeserializeBufTransform.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Protos/registry.pb.h>
#include <QueryPlan/PlanSerDerHelper.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BRPC_EXCEPTION;
    extern const int DISTRIBUTE_STAGE_QUERY_EXCEPTION;
}

BrpcRemoteBroadcastReceiver::BrpcRemoteBroadcastReceiver(
    ExchangeDataKeyPtr trans_key_,
    String registry_address_,
    ContextPtr context_,
    Block header_,
    bool keep_order_,
    const String & name_,
    BrpcExchangeReceiverRegistryService::RegisterMode mode_)
    : BrpcRemoteBroadcastReceiver(std::move(trans_key_)
    , std::move(registry_address_)
    , context_
    , std::move(header_)
    , keep_order_
    , name_
    , std::make_shared<MultiPathBoundedQueue>(context_->getSettingsRef().exchange_remote_receiver_queue_size, nullptr)
    , mode_)
{
}

BrpcRemoteBroadcastReceiver::BrpcRemoteBroadcastReceiver(
    ExchangeDataKeyPtr trans_key_,
    String registry_address_,
    ContextPtr context_,
    Block header_,
    bool keep_order_,
    const String & name_,
    MultiPathQueuePtr queue_,
    BrpcExchangeReceiverRegistryService::RegisterMode mode_,
    std::shared_ptr<QueryExchangeLog> query_exchange_log_,
    String coordinator_address_)
    : IBroadcastReceiver(context_->getSettingsRef().log_query_exchange)
    , name(name_)
    , trans_key(std::move(trans_key_))
    , registry_address(std::move(registry_address_))
    , context(std::move(context_))
    , header(std::move(header_))
    , queue(std::move(queue_))
    , keep_order(keep_order_)
    , initial_query_id(context->getInitialQueryId())
    , mode(mode_)
    , query_exchange_log(std::move(query_exchange_log_))
    , coordinator_address(std::move(coordinator_address_))
{
}

BrpcRemoteBroadcastReceiver::~BrpcRemoteBroadcastReceiver()
{
    try
    {
        if (stream_id != brpc::INVALID_STREAM_ID)
        {
            brpc::StreamClose(stream_id);
            LOG_TRACE(log, "Stream {} for {} @ {} Close", stream_id, name, registry_address);
        }
        if (!enable_receiver_metrics || !query_exchange_log)
            return;
        QueryExchangeLogElement element;
        element.initial_query_id = initial_query_id;
        element.exchange_id = trans_key->exchange_id;
        element.partition_id = trans_key->partition_id;
        element.event_time =
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        element.recv_counts = receiver_metrics.recv_counts.get_value();
        element.recv_time_ms = receiver_metrics.recv_time_ms.get_value();
        element.register_time_ms = receiver_metrics.register_time_ms.get_value();
        element.recv_rows = receiver_metrics.recv_rows.get_value();
        element.recv_bytes = receiver_metrics.recv_bytes.get_value();
        element.recv_uncompressed_bytes = receiver_metrics.recv_uncompressed_bytes.get_value();
        element.dser_time_ms = receiver_metrics.dser_time_ms.get_value();
        element.finish_code = receiver_metrics.finish_code;
        element.is_modifier = receiver_metrics.is_modifier;
        element.message = receiver_metrics.message;
        element.type = "brpc_receiver@reg_addr_" + registry_address;
        query_exchange_log->add(element);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void BrpcRemoteBroadcastReceiver::registerToSenders(UInt32 timeout_ms)
{
    Stopwatch s;
    std::shared_ptr<RpcClient> rpc_client = RpcChannelPool::getInstance().getClient(registry_address, BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY);
    Protos::RegistryService_Stub stub = Protos::RegistryService_Stub(&rpc_client->getChannel());
    brpc::Controller cntl;
    brpc::StreamOptions stream_options;
    stream_options.handler = std::make_shared<StreamHandler>(context, shared_from_this(), header, keep_order);
    if (timeout_ms == 0)
        cntl.set_timeout_ms(rpc_client->getChannel().options().timeout_ms);
    else
        cntl.set_timeout_ms(timeout_ms);
    cntl.set_max_retry(3);
    if (brpc::StreamCreate(&stream_id, cntl, &stream_options) != 0)
        throw Exception("Fail to create stream for " + getName(), ErrorCodes::BRPC_EXCEPTION);

    if (stream_id == brpc::INVALID_STREAM_ID)
        throw Exception("Stream id is invalid for " + getName(), ErrorCodes::BRPC_EXCEPTION);

    Protos::RegistryRequest request;
    Protos::RegistryResponse response;

    request.set_coordinator_address(coordinator_address);
    request.set_query_id(initial_query_id);
    request.set_query_unique_id(trans_key->query_unique_id);
    request.set_exchange_id(trans_key->exchange_id);
    request.set_parallel_id(trans_key->partition_id);
    request.set_parallel_index(trans_key->parallel_index);
    request.set_wait_timeout_ms(context->getSettingsRef().exchange_wait_accept_max_timeout_ms);
    request.set_stream_max_buf_size(context->getSettingsRef().exchange_stream_max_buf_size);
    sendRegisterRPC(stub, cntl, &request, &response, nullptr);

    // if exchange_enable_force_remote_mode = 1, sender and receiver in same process and sender stream may close before rpc end
    if (cntl.ErrorCode() == brpc::EREQUEST && cntl.ErrorText().ends_with("was closed before responded"))
    {
        LOG_DEBUG(
            log,
            "Receiver register sender successfully but sender already finished, host-{} , data_key-{}, stream_id-{}",
            registry_address,
            name,
            stream_id);
        return;
    }
    rpc_client->assertController(cntl);
    if (enable_receiver_metrics)
        receiver_metrics.register_time_ms << s.elapsedMilliseconds();
    LOG_DEBUG(log, "Receiver register sender successfully, host-{} , data_key-{}, stream_id-{}", registry_address, name, stream_id);
}

void BrpcRemoteBroadcastReceiver::pushReceiveQueue(MultiPathDataPacket packet)
{
    if (queue->closed())
        return;
    if (!queue->tryEmplaceUntil(context->getQueryExpirationTimeStamp(), std::move(packet)))
    {
        if(queue->closed())
        {
            return;
        }
        throw Exception(
            "Push exchange data to receiver for " + getName() + " timeout from "
                + DateLUT::serverTimezoneInstance().timeToString(context->getClientInfo().initial_query_start_time) + " to "
                + DateLUT::serverTimezoneInstance().timeToString(context->getQueryExpirationTimeStamp().tv_sec),
            ErrorCodes::DISTRIBUTE_STAGE_QUERY_EXCEPTION);
    }
}

RecvDataPacket BrpcRemoteBroadcastReceiver::recv(timespec timeout_ts)
{
    Stopwatch s;
    MultiPathDataPacket data_packet;
    if (!queue->tryPopUntil(data_packet, timeout_ts))
    {
        const auto error_msg = "Try pop receive queue for " + getName() + " timeout, from "
            + DateLUT::serverTimezoneInstance().timeToString(context->getClientInfo().initial_query_start_time) + " to "
            + DateLUT::serverTimezoneInstance().timeToString(timeout_ts.tv_sec);
        BroadcastStatus current_status = finish(BroadcastStatusCode::RECV_TIMEOUT, error_msg);
        return std::move(current_status);
    }

    if (std::holds_alternative<DataPacket>(data_packet))
    {
        auto & received_chunk = std::get<DataPacket>(data_packet).chunk;
        if (!received_chunk && !received_chunk.getChunkInfo())
        {
            LOG_TRACE(log, "{} finished ", getName());
            return RecvDataPacket(BroadcastStatus(BroadcastStatusCode::ALL_SENDERS_DONE, false, "receiver done"));
        }

        if (keep_order)
        {
            // Chunk in queue is created in StreamHanlder's on_received_messages callback, which is run in bthread.
            // Allocator (ref srcs/Common/Allocator.cpp) will add the momory of chunk to global memory tacker.
            // When this chunk is poped, we should add this memory to current query momory tacker, and subtract from global memory tacker.
            ExchangeUtils::transferGlobalMemoryToThread(received_chunk.allocatedBytes());
        }
        addToMetricsMaybe(s.elapsedMilliseconds(), 0, 1, received_chunk);
        return RecvDataPacket(std::move(received_chunk));
    }
    else
    {
        LOG_TRACE(log, "{} finished ", getName());
        return RecvDataPacket(BroadcastStatus(BroadcastStatusCode::ALL_SENDERS_DONE, false, "receiver done"));
    }
}

BroadcastStatus BrpcRemoteBroadcastReceiver::finish(BroadcastStatusCode status_code, String message)
{
    BroadcastStatusCode current_fin_code = finish_status_code.load(std::memory_order_relaxed);
    const auto *const msg = "BrpcRemoteBroadcastReceiver: already has been finished";
    if (current_fin_code != BroadcastStatusCode::RUNNING)
    {
        LOG_TRACE(
            log,
            "Broadcast {} finished and status can't be changed to {} any more. Current status: {}",
            name,
            status_code,
            current_fin_code);
        receiver_metrics.finish_code = current_fin_code;
        receiver_metrics.is_modifier = 0;
        return BroadcastStatus(current_fin_code, false, msg);
    }

    int actual_status_code = BroadcastStatusCode::RUNNING;

    BroadcastStatusCode new_fin_code = status_code;
    if (status_code < 0)
    {
        // if send_done_flag has never been set, sender should have some unkown errors.
        if (!send_done_flag.test(std::memory_order_acquire))
            new_fin_code = BroadcastStatusCode::SEND_UNKNOWN_ERROR;
    }

    if (finish_status_code.compare_exchange_strong(current_fin_code, new_fin_code, std::memory_order_relaxed, std::memory_order_relaxed))
    {
        if (new_fin_code > 0)
            queue->close();
        brpc::StreamFinish(stream_id, actual_status_code, new_fin_code, new_fin_code > 0);
        receiver_metrics.finish_code = new_fin_code;
        receiver_metrics.is_modifier = 1;
        receiver_metrics.message = std::move(message);
        if (new_fin_code > BroadcastStatusCode::RUNNING && new_fin_code != BroadcastStatusCode::RECV_CANCELLED
            && new_fin_code != BroadcastStatusCode::RECV_REACH_LIMIT)
        {
            LOG_ERROR(
                log,
                "Broadcast {} finished and changed to {} with err:'{}'",
                getName(),
                static_cast<BroadcastStatusCode>(new_fin_code),
                receiver_metrics.message);
        }
        else
        {
            LOG_TRACE(
                log,
                "Broadcast {} finished and changed to {} successfully. message:'{}'",
                getName(),
                static_cast<BroadcastStatusCode>(new_fin_code),
                receiver_metrics.message);
        }
        return BroadcastStatus(static_cast<BroadcastStatusCode>(new_fin_code), true, receiver_metrics.message);
    }
    else
    {
        LOG_TRACE(log, "Fail to change broadcast(name:{}) status to {}, current status is: {}", name, new_fin_code, current_fin_code);
        receiver_metrics.finish_code = current_fin_code;
        receiver_metrics.is_modifier = 0;
        return BroadcastStatus(current_fin_code, false, msg);
    }
}

String BrpcRemoteBroadcastReceiver::getName() const
{
    return name + ":" + trans_key->toString();
}

static void OnRegisterDone(Protos::RegistryResponse * /*response*/, brpc::Controller * cntl, std::function<void(void)> func)
{
    if (!cntl->Failed())
    {
        func();
    }
}

AsyncRegisterResult BrpcRemoteBroadcastReceiver::registerToSendersAsync(UInt32 timeout_ms)
{
    Stopwatch s;
    AsyncRegisterResult res;

    res.channel = RpcChannelPool::getInstance().getClient(registry_address, BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY);
    res.cntl = std::make_unique<brpc::Controller>();
    res.request = std::make_unique<Protos::RegistryRequest>();
    res.response = std::make_unique<Protos::RegistryResponse>();

    std::shared_ptr<RpcClient> & rpc_client = res.channel;
    Protos::RegistryService_Stub stub = Protos::RegistryService_Stub(&rpc_client->getChannel());

    brpc::Controller & cntl = *res.cntl;
    brpc::StreamOptions stream_options;
    stream_options.handler = std::make_shared<StreamHandler>(context, shared_from_this(), header, keep_order);

    if (timeout_ms == 0)
        cntl.set_timeout_ms(rpc_client->getChannel().options().timeout_ms);
    else
        cntl.set_timeout_ms(timeout_ms);
    cntl.set_max_retry(3);
    if (brpc::StreamCreate(&stream_id, cntl, &stream_options) != 0)
        throw Exception("Fail to create stream for " + getName(), ErrorCodes::BRPC_EXCEPTION);

    if (stream_id == brpc::INVALID_STREAM_ID)
        throw Exception("Stream id is invalid for " + getName(), ErrorCodes::BRPC_EXCEPTION);

    auto exchange_key = std::dynamic_pointer_cast<ExchangeDataKey>(trans_key);

    res.request->set_coordinator_address(coordinator_address);
    res.request->set_query_id(initial_query_id);
    res.request->set_query_unique_id(exchange_key->query_unique_id);
    res.request->set_exchange_id(exchange_key->exchange_id);
    res.request->set_parallel_id(exchange_key->partition_id);
    res.request->set_parallel_index(exchange_key->parallel_index);
    res.request->set_wait_timeout_ms(timeout_ms);
    res.request->set_stream_max_buf_size(context->getSettingsRef().exchange_stream_max_buf_size);
    std::function<void(void)> func = [&, s_cp = s]() {
        if (enable_receiver_metrics)
            receiver_metrics.register_time_ms << s_cp.elapsedMilliseconds();
    };
    sendRegisterRPC(
        stub, cntl, res.request.get(), res.response.get(), brpc::NewCallback(OnRegisterDone, res.response.get(), res.cntl.get(), func));
    LOG_TRACE(
        log,
        "name:{} addr:{} registerToSendersAsync costs {} ms, send rpc costs {} us",
        getName(),
        registry_address,
        s.elapsedMilliseconds(),
        cntl.latency_us());
    return res;
}

void BrpcRemoteBroadcastReceiver::sendRegisterRPC(
    Protos::RegistryService_Stub & stub,
    brpc::Controller & cntl,
    Protos::RegistryRequest * request,
    Protos::RegistryResponse * response,
    google::protobuf::Closure * done)
{
    if (mode == BrpcExchangeReceiverRegistryService::BRPC)
        stub.registry(&cntl, request, response, done);
    else if (mode == BrpcExchangeReceiverRegistryService::DISK_READER)
    {
        Protos::RegistryDiskSenderRequest disk_request;
        disk_request.mutable_registry()->Swap(request);
        serializeHeaderToProto(header, *disk_request.mutable_header());
        auto settings = context->getSettingsRef().dumpToMap();
        disk_request.mutable_settings()->insert(settings.begin(), settings.end());
        disk_request.set_initial_query_start_time(context->getClientInfo().initial_query_start_time_microseconds.value);
        stub.registerBRPCSenderFromDisk(&cntl, &disk_request, response, done);
    }
    else
        throw Exception(fmt::format("unrecognized mode ", mode), ErrorCodes::LOGICAL_ERROR);
}
}
