#include "ExchangeDataTracker.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Protos/plan_segment_manager.pb.h>
#include <Protos/registry.pb.h>
#include <Common/Exception.h>
#include <common/types.h>
#include "Interpreters/Context_fwd.h"
#include "Interpreters/DistributedStages/AddressInfo.h"
#include "Interpreters/DistributedStages/ExchangeMode.h"

namespace DB
{

std::vector<std::pair<size_t, ExchangeStatus>> fromSenderMetrics(const Protos::SenderMetrics & sender_metrics)
{
    std::vector<std::pair<size_t, ExchangeStatus>> ret;
    for (const auto & sb : sender_metrics.send_bytes())
    {
        ExchangeStatus status;
        status.worker_addr.fillFromProto(sender_metrics.address());
        status.status.resize(sb.bytes_by_index_size());
        for (const auto & b_i : sb.bytes_by_index())
        {
            status.status[b_i.parallel_index()] = b_i.bytes_sent();
        }
        ret.emplace_back(sb.exchange_id(), std::move(status));
    }
    return ret;
}

ExchangeStatusTracker::ExchangeStatusTracker(ContextWeakMutablePtr context_) : WithContext(context_)
{
}

void ExchangeStatusTracker::registerExchange(const String & query_id, UInt64 exchange_id, size_t parallel_size)
{
    LOG_TRACE(log, "register exchange for query:{} exchange_id:{} parallel_size:{}", query_id, exchange_id, parallel_size);
    std::lock_guard<std::mutex> g(exchange_status_mutex);
    query_exchange_ids[query_id].insert(exchange_id);
    const ExchangeKey ex_key({query_id, exchange_id});
    ExchangeStatuses ex_statuses = ExchangeStatuses{parallel_size};
    exchange_statuses.insert({std::move(ex_key), std::move(ex_statuses)});
}

void ExchangeStatusTracker::registerExchangeStatus(
    const String & query_id, UInt64 exchange_id, UInt64 parallel_index, const ExchangeStatus & status)
{
    LOG_TRACE(
        log,
        "register exchange status for query:{} exchange_id:{} parallel_index:{} status_size:{}",
        query_id,
        exchange_id,
        parallel_index,
        status.status.size());
    std::lock_guard<std::mutex> g(exchange_status_mutex);
    auto iter = exchange_statuses.find(ExchangeKey{query_id, exchange_id});
    if (iter == exchange_statuses.end())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "exchange statuses not registered for query_id:{} exchange_id:{} parallel_index:{}",
            query_id,
            exchange_id,
            parallel_index);

    iter->second.addStatus(parallel_index, status);
}

const ExchangeStatuses & ExchangeStatusTracker::getExchangeStatusesRef(const String & query_id, UInt64 exchange_id) const
{
    auto iter = exchange_statuses.find(ExchangeKey{query_id, exchange_id});
    if (iter == exchange_statuses.end())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "exchange statuses not registered for query_id:{} exchange_id:{}", query_id, exchange_id);

    return iter->second;
}

void ExchangeStatusTracker::unregisterExchange(const String & query_id, UInt64 exchange_id)
{
    exchange_statuses.erase(ExchangeKey{query_id, exchange_id});
}

void ExchangeStatusTracker::unregisterExchanges(const String & query_id)
{
    std::lock_guard<std::mutex> g(exchange_status_mutex);
    for (const auto & s_id : query_exchange_ids[query_id])
    {
        unregisterExchange(query_id, s_id);
    }
    query_exchange_ids.erase(query_id);
}

bool ExchangeStatusTracker::checkQueryAlive(const String & query_id)
{
    std::lock_guard<std::mutex> g(exchange_status_mutex);
    auto iter = query_exchange_ids.find(query_id);
    return iter != query_exchange_ids.end();
}

size_t ExchangeStatusTracker::getExchangeDataSize(const String & query_id, UInt64 exchange_id) const
{
    const auto & statuses = getExchangeStatusesRef(query_id, exchange_id);
    size_t sum = 0;
    for (const auto & status : statuses.getStatusesRef())
    {
        for (const auto & bytes : status.status)
        {
            sum += bytes;
        }
    }
    return sum;
}

std::vector<AddressInfo> ExchangeStatusTracker::getExchangeDataAddrs(
    PlanSegment * plan_segment, UInt64 start_parallel_index, UInt64 end_parallel_index, double locality_fraction)
{
    std::vector<AddressInfo> addrs;
    std::unordered_map<UInt64, std::unordered_map<AddressInfo, size_t, AddressInfo::Hash>> acc;
    for (const auto & input : plan_segment->getPlanSegmentInputs())
    {
        // We skip the local table scan and those broadcast inputs whose size is relative small.
        if (input->getExchangeMode() == ExchangeMode::UNKNOWN || input->getExchangeMode() == ExchangeMode::BROADCAST)
            continue;
        chassert(
            input->getExchangeMode() != ExchangeMode::LOCAL_NO_NEED_REPARTITION
            && input->getExchangeMode() != ExchangeMode::LOCAL_MAY_NEED_REPARTITION);
        std::lock_guard<std::mutex> g(exchange_status_mutex);
        const auto & statuses = getExchangeStatusesRef(plan_segment->getQueryId(), input->getExchangeId());
        for (const auto & status : statuses.getStatusesRef())
        {
            for (UInt64 i = start_parallel_index; i < end_parallel_index; i++)
            {
                auto & partition_acc = acc[i];
                auto & s = partition_acc[status.worker_addr];
                if (s)
                {
                    s += status.status.at(i);
                }
                else
                {
                    s = status.status.at(i);
                }
            }
        }
    }

    for (UInt64 i = start_parallel_index; i < end_parallel_index; i++)
    {
        const auto & partition_acc = acc[i];
        size_t max_size = 0;
        AddressInfo cur_addr;
        bool decided = false;
        size_t total_size = 0;
        for (const auto & [_, output_size] : partition_acc)
        {
            total_size += output_size;
        }
        for (const auto & [addr, output_size] : partition_acc)
        {
            // TODO(WangTao): support multi candidate nodes.
            if (output_size >= max_size && total_size > 0 && static_cast<double>(output_size) / total_size > locality_fraction)
            {
                max_size = output_size;
                cur_addr = addr;
                decided = true;
            }
        }
        if (decided)
        {
            addrs.emplace_back(cur_addr);
        }
        else
        {
            addrs.emplace_back(AddressInfo{});
        }
    }

    return addrs;
}

} // namespace DB
