#include <Interpreters/RuntimeFilter/RuntimeFilterConsumer.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Protos/runtime_filter.pb.h>
#include <brpc/server.h>
#include <Common/Brpc/BrpcChannelPoolOptions.h>
#include <Common/LinkedHashMap.h>

namespace DB
{
RuntimeFilterConsumer::RuntimeFilterConsumer(
    std::shared_ptr<RuntimeFilterBuilder> builder_,
    std::string query_id_,
    size_t local_stream_parallel_,
    size_t parallel_,
    size_t grf_ndv_enlarge_size_,
    AddressInfo coordinator_address_,
    AddressInfo current_address_)
    : builder(std::move(builder_))
    , query_id(std::move(query_id_))
    , local_stream_parallel(local_stream_parallel_)
    , parallel(parallel_)
    , grf_ndv_enlarge_size(grf_ndv_enlarge_size_)
    , coordinator_address(std::move(coordinator_address_))
    , current_address(std::move(current_address_))
    , timer{CLOCK_MONOTONIC_COARSE}
    , log(&Poco::Logger::get("RuntimeFilterBuild"))
{
    timer.start();
}


void RuntimeFilterConsumer::addFinishRuntimeFilter(RuntimeFilterData && data, bool is_local)
{
    std::lock_guard<std::mutex> guard(mutex);
    runtime_filters.emplace_back(std::move(data));
    // no need to unlock since all other streams are finished.
    if (runtime_filters.size() == local_stream_parallel)
    {
        if (is_local)
        {
            if (runtime_filters.size() == 1)
            {
                // no merge need
                for (auto && dy : runtime_filters[0].runtime_filters)
                {
                    DynamicData dd;
                    dd.is_local = true;
                    dd.data = std::move(dy.second);
                    RuntimeFilterManager::getInstance().addDynamicValue(query_id, dy.first, std::move(dd), 1);
                }
            }
            else
            {
                auto && dynamic_data = builder->merge(runtime_filters);
                if (dynamic_data.bypass != BypassType::NO_BYPASS)
                {
                    for (const auto & rf: builder->getRuntimeFilters())
                    {
                        DynamicData local_data;
                        local_data.is_local = true;
                        local_data.bypass = local_data.bypass;
                        RuntimeFilterManager::getInstance().addDynamicValue(query_id, rf.second.id, std::move(local_data), 1);
                    }
                }
                else
                {
                    for (auto && dy : dynamic_data.runtime_filters)
                    {
                        DynamicData dd;
                        dd.is_local = true;
                        dd.data = std::move(dy.second);
                        RuntimeFilterManager::getInstance().addDynamicValue(query_id, dy.first, std::move(dd), 1);
                    }
                }
            }
        }
        else
        {
            if (runtime_filters.size() == 1)
                transferRuntimeFilter(std::move(runtime_filters[0]));
            else
                transferRuntimeFilter(builder->merge(runtime_filters));
        }
    }
}

void RuntimeFilterConsumer::addFinishRF(BloomFilterWithRangePtr && bf_ptr, RuntimeFilterId id, bool is_local)
{
    RuntimeFilterVal val;
    val.is_bf = true;
    val.bloom_filter = std::move(bf_ptr);
    RuntimeFilterData data;
    data.runtime_filters.emplace(id, std::move(val));
    addFinishRuntimeFilter(std::move(data), is_local);
}

void RuntimeFilterConsumer::addFinishRF(ValueSetWithRangePtr && vs_ptr, RuntimeFilterId id, bool is_local)
{
    RuntimeFilterVal val;
    val.is_bf = false;
    val.values_set = std::move(vs_ptr);
    RuntimeFilterData data;
    data.runtime_filters.emplace(id, std::move(val));
    addFinishRuntimeFilter(std::move(data), is_local);
}

bool RuntimeFilterConsumer::isBloomFilter(DB::RuntimeFilterId id) const
{
    if (!runtime_filters.empty())
    {
        for (const auto & rf : runtime_filters)
            if (rf.isBloomFilter(id))
                return true;
    }

    return false;
}

bool RuntimeFilterConsumer::isValueSet(DB::RuntimeFilterId id) const
{
    if (!runtime_filters.empty())
    {
        for (const auto & rf : runtime_filters)
            if (rf.isValueSet(id))
                return true;
    }

    return false;
}

void RuntimeFilterConsumer::bypass(RuntimeFilterId id, bool is_local, BypassType type)
{
    // single empty or large bypass
    if (local_stream_parallel == 1)
    {
        if (is_local)
        {
            DynamicData data;
            data.is_local = true;
            data.bypass = type;
            RuntimeFilterManager::getInstance().addDynamicValue(query_id, id, std::move(data), 1);
        }
        else
        {
            RuntimeFilterData data;
            data.bypass = type;
            transferRuntimeFilter(std::move(data));
        }
    }
    else
    {
        // more than one ht, need merge final
        RuntimeFilterData data;
        data.bypass = type;
        data.runtime_filters.emplace(id, RuntimeFilterVal{});
        addFinishRuntimeFilter(std::move(data), is_local);
    }
}

static void OnSendRuntimeFilterCallback(
    Protos::TransferRuntimeFilterResponse * response, brpc::Controller * cntl, std::shared_ptr<RpcClient> rpc_channel)
{
    std::unique_ptr<Protos::TransferRuntimeFilterResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);

    rpc_channel->checkAliveWithController(*cntl);
    if (cntl->Failed())
        LOG_DEBUG(&Poco::Logger::get("RuntimeFilterBuild"), "Send to coordinator failed, message: " + cntl->ErrorText());
    else
        LOG_DEBUG(&Poco::Logger::get("RuntimeFilterBuild"), "Send to coordinator success");
}

void RuntimeFilterConsumer::transferRuntimeFilter(RuntimeFilterData && data)
{
    WriteBufferFromOwnString write_buffer;
    data.serialize(write_buffer);
    write_buffer.next();

    auto * response = new Protos::TransferRuntimeFilterResponse;
    auto * controller = new brpc::Controller;
    Protos::TransferRuntimeFilterRequest request;
    std::shared_ptr<RpcClient> rpc_client = RpcChannelPool::getInstance().getClient(
        extractExchangeStatusHostPort(coordinator_address), BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, true);
    Protos::RuntimeFilterService_Stub runtime_filter_service(&rpc_client->getChannel());
    request.set_query_id(query_id);
    request.set_builder_id(builder->getId());
    request.set_worker_address(extractExchangeStatusHostPort(current_address));
    request.set_require_parallel_size(parallel);
    request.set_filter_data(write_buffer.str());
    runtime_filter_service.transferRuntimeFilter(
        controller, &request, response, brpc::NewCallback(OnSendRuntimeFilterCallback, response, controller, rpc_client));

    LOG_DEBUG(
        log,
        "build success, query id: {}, builer id: {}, stream parallel: {}, plan segment parallel: {}, cost: {} ms",
        query_id,
        builder->getId(),
        local_stream_parallel,
        parallel,
        timer.elapsedMilliseconds());
}


}
