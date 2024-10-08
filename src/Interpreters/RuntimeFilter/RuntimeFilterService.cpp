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

#include <Interpreters/RuntimeFilter/RuntimeFilterService.h>

#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Interpreters/SegmentScheduler.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <QueryPlan/PlanSerDerHelper.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void
onDispatchRuntimeFilter(
    LoggerPtr log, Protos::DispatchRuntimeFilterResponse * response, brpc::Controller * cntl, std::shared_ptr<RpcClient> rpc_channel)
{
    std::unique_ptr<Protos::DispatchRuntimeFilterResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);

    rpc_channel->checkAliveWithController(*cntl);
    if (cntl->Failed())
        LOG_DEBUG(log, "dispatch runtime filter to worker failed, message: " + cntl->ErrorText());
    else
        LOG_DEBUG(log, "dispatch runtime filter to worker success");
}

void RuntimeFilterService::transferRuntimeFilter(
    ::google::protobuf::RpcController * /*controller*/,
    const ::DB::Protos::TransferRuntimeFilterRequest * request,
    ::DB::Protos::TransferRuntimeFilterResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    Stopwatch timer{CLOCK_MONOTONIC_COARSE};
    timer.start();

    brpc::ClosureGuard done_guard(done);
    try
    {
        auto segment_scheduler = context->getSegmentScheduler();
        if (!segment_scheduler)
            return;

        ReadBufferFromMemory read_buffer(request->filter_data().c_str(), request->filter_data().size());
        RuntimeFilterData data;
        data.deserialize(read_buffer);

        auto & manager = RuntimeFilterManager::getInstance();

        auto collection_context = manager.getRuntimeFilterCollectionContext(request->query_id());
        auto collection = collection_context->getCollection(request->builder_id());
        size_t received = collection->add(std::move(data), request->parallel_id());
        size_t required = collection->getParallelSize();

        LOG_TRACE(
            log,
            "coordinator receive query id: {}, builder id: {}, {} / {} with {} ms",
            request->query_id(),
            request->builder_id(),
            received,
            required,
            timer.elapsedMilliseconds());

        if (received == required)
        {
            timer.restart();
            LOG_DEBUG(
                log,
                "coordinator receive all runtime filters form builder workers, query id: {}, builder id: {}, "
                "try to dispatch to execute plan segments.",
                request->query_id(),
                request->builder_id());

            std::unordered_map<RuntimeFilterId, InternalDynamicData> && dynamic_values = collection->finalize();
            for (const auto & [filter_id, field] : dynamic_values)
            {
                WriteBufferFromOwnString write_buffer;
                writeFieldBinary(field.range, write_buffer);
                writeFieldBinary(field.bf, write_buffer);
                writeFieldBinary(field.set, write_buffer);
                writeBinary(static_cast<UInt8>(field.bypass), write_buffer);

                std::map<AddressInfo, std::unordered_set<RuntimeFilterId>> send_addresses;
                const auto & execute_segment_ids = collection_context->getExecuteSegmentIds(filter_id);
                for (const auto & segment_id : execute_segment_ids)
                {
                    AddressInfos worker_addresses = segment_scheduler->getWorkerAddress(request->query_id(), segment_id);
                    for (const auto & address : worker_addresses)
                        send_addresses[address].emplace(segment_id);
                }

                Protos::DispatchRuntimeFilterRequest dispatch_request;
                dispatch_request.set_query_id(request->query_id());
                dispatch_request.set_filter_id(filter_id);
                dispatch_request.set_filter_data(write_buffer.str());

                for (const auto & [address, segment_ids] : send_addresses)
                {
                    dispatch_request.set_ref_segment(segment_ids.size());

                    String host_port = extractExchangeHostPort(address);
                    std::shared_ptr<RpcClient> rpc_client
                        = RpcChannelPool::getInstance().getClient(host_port, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
                    std::shared_ptr<DB::Protos::RuntimeFilterService_Stub> command_service
                        = std::make_shared<Protos::RuntimeFilterService_Stub>(&rpc_client->getChannel());
                    auto * controller = new brpc::Controller;
                    auto * dispatch_response = new Protos::DispatchRuntimeFilterResponse;
                    command_service->dispatchRuntimeFilter(
                        controller,
                        &dispatch_request,
                        dispatch_response,
                        brpc::NewCallback(onDispatchRuntimeFilter, log, dispatch_response, controller, rpc_client));

                    LOG_DEBUG(log, "coordinator dispatch query id: {}, filter id: {}, host: {}", request->query_id(), filter_id, host_port);
                }
            }

            LOG_DEBUG(log, "coordinator dispatch all runtime filters to worker with {} ms", timer.elapsedMilliseconds());
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void RuntimeFilterService::dispatchRuntimeFilter(
    ::google::protobuf::RpcController * /*controller*/,
    const ::DB::Protos::DispatchRuntimeFilterRequest * request,
    ::DB::Protos::DispatchRuntimeFilterResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        Stopwatch timer{CLOCK_MONOTONIC_COARSE};
        timer.start();
        ReadBufferFromMemory read_buffer(request->filter_data().c_str(), request->filter_data().size());
        InternalDynamicData field;

        readFieldBinary(field.range, read_buffer);
        readFieldBinary(field.bf, read_buffer);
        readFieldBinary(field.set, read_buffer);
        UInt8 t;
        readBinary(t, read_buffer);
        field.bypass = BypassType(t);

        DynamicData dynamic_data;
        dynamic_data.bypass = field.bypass;
        dynamic_data.data = std::move(field);
        LOG_DEBUG(
            log,
            "probe worker receive runtime filter value, query id: {}, filter id: {}, deserialize cost {} ms",
            request->query_id(),
            request->filter_id(),
            timer.elapsedMilliseconds());

        RuntimeFilterManager::getInstance().addDynamicValue(
            request->query_id(), request->filter_id(), std::move(dynamic_data), request->ref_segment());
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}
}
