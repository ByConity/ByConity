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

#include "BrpcExchangeReceiverRegistryService.h"
#include <cstdint>
#include <memory>
#include <sstream>

#include <DataStreams/NativeBlockInputStream.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/ExchangeDataTracker.h>
#include <Processors/Exchange/DataTrans/Batch/DiskExchangeDataManager.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/Brpc/AsyncRegisterResult.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastSender.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <QueryPlan/PlanSerDerHelper.h>
#include <brpc/stream.h>
#include <Common/Exception.h>
#include <Common/SettingsChanges.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BRPC_EXCEPTION;
}

void BrpcExchangeReceiverRegistryService::registry(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::RegistryRequest * request,
    ::DB::Protos::RegistryResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::StreamId sender_stream_id = brpc::INVALID_STREAM_ID;
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    auto key = std::make_shared<ExchangeDataKey>(request->query_unique_id(), request->exchange_id(), request->parallel_id());
    BroadcastSenderProxyPtr sender_proxy = BroadcastSenderProxyRegistry::instance().getOrCreate(key);
    auto query_id = request->query_id();
    auto coordinator_addr = request->coordinator_address();
    /// SCOPE_EXIT wrap logic which run after done->Run(),
    /// since host socket of the accpeted stream is set in done->Run()
    SCOPE_EXIT({
        /// request is already released in done_guard, so all parameters have to be copied.
        if (sender_proxy && sender_stream_id != brpc::INVALID_STREAM_ID)
            registerSenderToProxy(nullptr, sender_proxy, query_id, sender_stream_id, {}, key, coordinator_addr, false);
    });

    /// this done_guard guarantee to call done->Run() in any situation
    brpc::ClosureGuard done_guard(done);
    auto accept_timeout_ms = request->wait_timeout_ms();
    auto stream_max_buf_size
        = request->has_stream_max_buf_size() ? request->stream_max_buf_size() : context->getSettingsRef().exchange_stream_max_buf_size;
    acceptStream(cntl, accept_timeout_ms, stream_max_buf_size, sender_proxy, request->query_id(), sender_stream_id);
}

void BrpcExchangeReceiverRegistryService::registerBRPCSenderFromDisk(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::RegistryDiskSenderRequest * request,
    ::DB::Protos::RegistryResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    ExchangeDataKeyPtr key;
    try
    {
        chassert(request->has_header());
        brpc::StreamId sender_stream_id = brpc::INVALID_STREAM_ID;
        /// SCOPE_EXIT wrap logic which run after done->Run(),
        /// since host socket of the accpeted stream is set in done->Run()
        key = std::make_shared<ExchangeDataKey>(
            request->registry().query_unique_id(),
            request->registry().exchange_id(),
            request->registry().parallel_id(),
            request->registry().parallel_index());
        Block header = deserializeHeaderFromProto(request->header());
        auto mgr = context->getDiskExchangeDataManager();
        auto query_context = Context::createCopy(context);
        SettingsChanges settings_changes;
        settings_changes.reserve(request->settings_size());
        for (const auto & [setting_key, setting_value] : request->settings())
        {
            settings_changes.push_back({setting_key, setting_value});
        }
        query_context->applySettingsChanges(settings_changes);
        ClientInfo & client_info = query_context->getClientInfo();
        Decimal64 initial_query_start_time_microseconds{request->initial_query_start_time()};
        client_info.initial_query_id = request->registry().query_id(); /// needed for query exchange log initial_query_id
        client_info.initial_query_start_time = initial_query_start_time_microseconds / 1000000;
        client_info.initial_query_start_time_microseconds = initial_query_start_time_microseconds;
        query_context->initQueryExpirationTimeStamp();
        auto query_id = request->registry().query_id();
        auto coordinator_addr = request->registry().coordinator_address();
        /// we need to do this as to avoid previous sender waitBecomeRealSender in finish
        auto previous_sender = BroadcastSenderProxyRegistry::instance().get(key);
        if (previous_sender)
        {
            previous_sender->finish(BroadcastStatusCode::SEND_CANCELLED, "cancelled as previous sender");
            BroadcastSenderProxyRegistry::instance().remove(key);
            LOG_WARNING(log, "previous_sender found for query_id:{} key:{}", query_id, *key);
        }
        mgr->cancelReadTask(key); /// cancel possible previous read task from last execution
        previous_sender = nullptr; // dont forget to release
        BroadcastSenderProxyPtr sender_proxy = BroadcastSenderProxyRegistry::instance().getOrCreate(key);
        sender_proxy->accept(query_context, header);
        auto processors = mgr->createProcessors(sender_proxy, std::move(header), std::move(query_context));
        SCOPE_EXIT({
            if (sender_proxy && sender_stream_id != brpc::INVALID_STREAM_ID)
                registerSenderToProxy(mgr, sender_proxy, query_id, sender_stream_id, processors, key, coordinator_addr, true);
        });

        /// this done_guard guarantee to call done->Run() in any situation
        brpc::ClosureGuard done_guard(done);
        auto accept_timeout_ms = request->registry().wait_timeout_ms();
        auto stream_max_buf_size = request->registry().has_stream_max_buf_size() ? request->registry().stream_max_buf_size()
                                                                                 : context->getSettingsRef().exchange_stream_max_buf_size;
        acceptStream(cntl, accept_timeout_ms, stream_max_buf_size, sender_proxy, request->registry().query_id(), sender_stream_id);
    }
    catch (...)
    {
        String error_msg = fmt::format("registerBRPCSenderFromDisk failed for key:{} query:{} ", *key, request->registry().query_id());
        LOG_ERROR(log, error_msg);
        cntl->SetFailed(error_msg);
    }
}

void BrpcExchangeReceiverRegistryService::registerSenderToProxy(
    const DiskExchangeDataManagerPtr & mgr,
    const BroadcastSenderProxyPtr & sender_proxy,
    const String & query_id,
    const brpc::StreamId & sender_stream_id,
    Processors processors,
    const ExchangeDataKeyPtr & key,
    const String & coordinator_addr,
    bool read_from_disk)
{
    try
    {
        if (read_from_disk)
            mgr->cancelReadTask(sender_proxy->getDataKey());
        auto real_sender = std::dynamic_pointer_cast<IBroadcastSender>(std::make_shared<BrpcRemoteBroadcastSender>(
            sender_proxy->getDataKey(), sender_stream_id, sender_proxy->getContext(), sender_proxy->getHeader()));
        sender_proxy->becomeRealSender(std::move(real_sender));
        /// submit executor task
        if (read_from_disk)
        {
            mgr->submitReadTask(query_id, sender_proxy->getDataKey(), std::move(processors), coordinator_addr);
            LOG_TRACE(log, fmt::format("Submit read task for query {} key {} successfully", query_id, *key));
        }
    }
    catch (...)
    {
        brpc::StreamClose(sender_stream_id);
        String err_msg = fmt::format(
            "registerSenderToProxy failed for query_id:{} key:{} by exception: {}", query_id, *key, getCurrentExceptionMessage(false));
        LOG_ERROR(log, err_msg);
    }
}

void BrpcExchangeReceiverRegistryService::acceptStream(
    brpc::Controller * cntl,
    uint64_t accept_timeout_ms,
    uint64_t max_buf_size,
    BroadcastSenderProxyPtr sender,
    const String & query_id,
    brpc::StreamId & sender_stream_id)
{
    brpc::StreamOptions stream_options;
    stream_options.max_buf_size = max_buf_size;
    auto key = sender->getDataKey();
    try
    {
        sender->waitAccept(accept_timeout_ms);
        if (brpc::StreamAccept(&sender_stream_id, *cntl, &stream_options) != 0)
        {
            sender_stream_id = brpc::INVALID_STREAM_ID;
            String error_msg = "Fail to accept stream " + key->toString() + " for query " + query_id;
            LOG_ERROR(log, error_msg);
            cntl->SetFailed(error_msg);
        }
    }
    catch (...)
    {
        String error_msg
            = "Create stream " + key->toString() + " for query " + query_id + " failed by exception: " + getCurrentExceptionMessage(false);
        LOG_ERROR(log, error_msg);
        cntl->SetFailed(error_msg);
    }
}

void BrpcExchangeReceiverRegistryService::cancelExchangeDataReader(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::CancelExchangeDataReaderRequest * request,
    ::DB::Protos::CancelExchangeDataReaderResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    try
    {
        auto mgr = context->getDiskExchangeDataManager();
        mgr->cancelReadTask(request->query_unique_id(), request->exchange_id());
    }
    catch (Exception & e)
    {
        String error_msg = fmt::format(
            "Cancel disk reader failed for query id:{} exchange id:{} exception:{}",
            request->query_unique_id(),
            request->exchange_id(),
            e.message());
        tryLogCurrentException(log, error_msg);
        cntl->SetFailed(error_msg);
    }
}

void BrpcExchangeReceiverRegistryService::cleanupExchangeData(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::CleanupExchangeDataRequest * request,
    ::DB::Protos::CleanupExchangeDataResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    try
    {
        LOG_TRACE(log, "submit cleanup task for query_unique_id:{} successfully", request->query_unique_id());
        auto mgr = context->getDiskExchangeDataManager();
        mgr->submitCleanupTask(request->query_unique_id());
    }
    catch (...)
    {
        auto error_msg = fmt::format("submit cleanup exchange data failed for query_unique_id:{}", request->query_unique_id());
        tryLogCurrentException(log, error_msg);
        cntl->SetFailed(error_msg);
    }
}

void BrpcExchangeReceiverRegistryService::sendExchangeDataHeartbeat(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::ExchangeDataHeartbeatRequest * request,
    ::DB::Protos::ExchangeDataHeartbeatResponse * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    try
    {
        auto exchange_data_tracker = context->getExchangeDataTracker();
        for (const auto & info : request->infos())
        {
            if (!exchange_data_tracker->checkQueryAlive(info.query_id()))
            {
                auto * query_info_resp = response->add_not_alive_queries();
                query_info_resp->set_query_id(info.query_id());
                query_info_resp->set_query_unique_id(info.query_unique_id());
            }
        }
    }
    catch (...)
    {
        std::stringstream ss;
        for (const auto & info : request->infos())
            ss << "query_unique_id:" << info.query_unique_id() << " query_id:" << info.query_id() << std::endl;
        auto error_msg = fmt::format("sendExchangeFileHeartbeat exchange data failed for queries:{}", ss.str());
        tryLogCurrentException(log, error_msg);
        cntl->SetFailed(error_msg);
    }
}
}
