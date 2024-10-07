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
    AddressInfo coordinator_address_,
    UInt32 parallel_id_)
    : builder(std::move(builder_))
    , query_id(std::move(query_id_))
    , local_stream_parallel(local_stream_parallel_)
    , parallel_workers(parallel_)
    , coordinator_address(std::move(coordinator_address_))
    , parallel_id(parallel_id_)
    , build_params_blocks(local_stream_parallel)
    , timer{CLOCK_MONOTONIC_COARSE}
    , log(getLogger("RuntimeFilterBuild"))
{
    timer.start();
}

bool RuntimeFilterConsumer::addBuildParams(size_t ht_size, const DB::BlocksList * blocks)
{
    auto index = num_partial.fetch_add(1, std::memory_order_relaxed);
    build_params_blocks[index] = blocks;
    ht_sizes.fetch_add(ht_size, std::memory_order_relaxed);
    return static_cast<size_t>(index) == (local_stream_parallel - 1);
}

void RuntimeFilterConsumer::addFinishRF(BloomFilterWithRangePtr && bf_ptr, RuntimeFilterId id, bool is_local)
{
    RuntimeFilterVal val;
    val.is_bf = true;
    val.bloom_filter = std::move(bf_ptr);
    if (is_local)
    {
        DynamicData dd;
        dd.is_local = true;
        dd.data = std::move(val);
        RuntimeFilterManager::getInstance().addDynamicValue(query_id, id, std::move(dd), 1);
    }
    else
    {
        global_rf_data.runtime_filters.emplace(id, std::move(val));
    }
}

void RuntimeFilterConsumer::addFinishRF(ValueSetWithRangePtr && vs_ptr, RuntimeFilterId id, bool is_local)
{
    RuntimeFilterVal val;
    val.is_bf = false;
    val.values_set = std::move(vs_ptr);
    if (is_local)
    {
        DynamicData dd;
        dd.is_local = true;
        dd.data = std::move(val);
        RuntimeFilterManager::getInstance().addDynamicValue(query_id, id, std::move(dd), 1);
    }
    else
    {
        global_rf_data.runtime_filters.emplace(id, std::move(val));
    }
}

void RuntimeFilterConsumer::finalize()
{
    if (!global_rf_data.runtime_filters.empty())
    {
        transferRuntimeFilter(std::move(global_rf_data));
    }
}

void RuntimeFilterConsumer::bypass(BypassType type)
{
    if (is_bypassed)
        return;
    is_bypassed = true;
    const auto & runtime_filters = builder->getRuntimeFilters();
    bool need_send_global = false;
    if (!runtime_filters.empty())
    {
        for (const auto & rf : runtime_filters)
        {
            if (rf.second.distribution == RuntimeFilterDistribution::LOCAL)
            {
                DynamicData data;
                data.is_local = true;
                data.bypass = type;
                RuntimeFilterManager::getInstance().addDynamicValue(query_id, rf.second.id, std::move(data), 1);
            }
            else
            {
                need_send_global = true;
            }
        }

        if (need_send_global)
        {
            RuntimeFilterData data;
            data.bypass = type;
            transferRuntimeFilter(std::move(data));
        }
    }
}

static void OnSendRuntimeFilterCallback(
    Protos::TransferRuntimeFilterResponse * response, brpc::Controller * cntl, std::shared_ptr<RpcClient> rpc_channel)
{
    std::unique_ptr<Protos::TransferRuntimeFilterResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);

    rpc_channel->checkAliveWithController(*cntl);
    if (cntl->Failed())
        LOG_DEBUG(getLogger("RuntimeFilterBuild"), "Send to coordinator failed, message: " + cntl->ErrorText());
    else
        LOG_DEBUG(getLogger("RuntimeFilterBuild"), "Send to coordinator success");
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
        extractExchangeHostPort(coordinator_address), BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
    Protos::RuntimeFilterService_Stub runtime_filter_service(&rpc_client->getChannel());
    request.set_query_id(query_id);
    request.set_builder_id(builder->getId());
    request.set_parallel_id(parallel_id);
    request.set_require_parallel_size(parallel_workers);
    request.set_filter_data(write_buffer.str());
    runtime_filter_service.transferRuntimeFilter(
        controller, &request, response, brpc::NewCallback(OnSendRuntimeFilterCallback, response, controller, rpc_client));

    LOG_DEBUG(
        log,
        "build rf success, query id: {}, build id: {}, stream parallel: {}, plan segment parallel: {}, cost: {} ms",
        query_id,
        builder->getId(),
        local_stream_parallel,
        parallel_id,
        timer.elapsedMilliseconds());
}


}
