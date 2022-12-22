#include <Common/config.h>
#if USE_RDKAFKA

#include <Storages/Kafka/CnchKafkaConsumerScheduler.h>

namespace DB
{
    IKafkaConsumerScheduler::IKafkaConsumerScheduler(const String &vw_name_, const KafkaConsumerScheduleMode schedule_mode_, ContextPtr context_)
        : vw_name(std::move(vw_name_)), schedule_mode(schedule_mode_), global_context(context_->getGlobalContext()),
        log(&Poco::Logger::get("KafkaConsumer" + String(getScheduleModeName()) + "Scheduler"))
    {
        initOrUpdateWorkerPool();
    }

    void IKafkaConsumerScheduler::initOrUpdateWorkerPool()
    {
        /// Here updating for `worker_pool` will be triggered
        if (worker_pool && !worker_pool->empty())
            return;

        worker_pool = global_context->getCnchWorkerClientPools().getPool({vw_name},
                                                                 {VirtualWarehouseType::Write, VirtualWarehouseType::Default});
        if (!worker_pool || worker_pool->empty())
            throw Exception("Init worker pool #" + vw_name + " failed, pls check it", ErrorCodes::LOGICAL_ERROR);
    }

    /// Random mode:
    KafkaConsumerSchedulerRandom::KafkaConsumerSchedulerRandom(const String &vw_name_,
                                                               const KafkaConsumerScheduleMode schedule_mode_,
                                                               ContextPtr global_context_)
        : IKafkaConsumerScheduler(vw_name_, schedule_mode_, global_context_)
    {}

    CnchWorkerClientPtr KafkaConsumerSchedulerRandom::selectWorkerNode(const String & /* key */, const size_t /* index */)
    {
        initOrUpdateWorkerPool();
        return worker_pool->get();
    }

    /// Hash mode:
    KafkaConsumerSchedulerHash::KafkaConsumerSchedulerHash(const String &vw_name_,
                                                           const KafkaConsumerScheduleMode schedule_mode_,
                                                           ContextPtr global_context_)
        : IKafkaConsumerScheduler(vw_name_, schedule_mode_, global_context_)
    {}

    CnchWorkerClientPtr KafkaConsumerSchedulerHash::selectWorkerNode(const String &key, const size_t /* index */)
    {
        initOrUpdateWorkerPool();
        return worker_pool->getByHashRing(key);
    }

    bool KafkaConsumerSchedulerHash::shouldReschedule(const CnchWorkerClientPtr current_worker, const String & key, const size_t index)
    {
        initOrUpdateWorkerPool();
        CnchWorkerClientPtr new_worker = selectWorkerNode(key, index);

        if (new_worker->getRPCAddress() == current_worker->getRPCAddress())
            return false;

        /// New worker should have run for some time to ensure its stability
        /// TODO: @renqiang A more graceful and reliable method should be introduced
        if (new_worker->getActiveTime() < min_running_time_for_reschedule)
            return false;

        return true;
    }

    /// LeastConsumers mode
    KafkaConsumerSchedulerLeastConsumers::KafkaConsumerSchedulerLeastConsumers(const String &vw_name_,
                                                           const KafkaConsumerScheduleMode schedule_mode_,
                                                           ContextPtr global_context_)
        : IKafkaConsumerScheduler(vw_name_, schedule_mode_, global_context_)
    {}

    void KafkaConsumerSchedulerLeastConsumers::initOrUpdateWorkerPool()
    {
        IKafkaConsumerScheduler::initOrUpdateWorkerPool();

        auto new_clients = worker_pool->getAll();
        std::unordered_map<CnchWorkerClientPtr, size_t> new_clients_map;
        std::priority_queue<client_status> new_clients_queue;

        for (auto & client : new_clients)
        {
            size_t cnt = 0;
            auto iter = clients_status_map.find(client);
            if (iter != clients_status_map.end())
                cnt = iter->second;

            new_clients_map.emplace(client, cnt);
            new_clients_queue.emplace(client, cnt);
        }

        clients_queue = std::move(new_clients_queue);
        clients_status_map = std::move(new_clients_map);
    }

    /// FIXME: @renqiang if the worker node has been down and not be removed from vw, it will also be selected
    CnchWorkerClientPtr KafkaConsumerSchedulerLeastConsumers::selectWorkerNode(const String & /* key */, const size_t /* index */)
    {
        initOrUpdateWorkerPool();

        while (!clients_queue.empty())
        {
            auto node = clients_queue.top();
            clients_queue.pop();

            if (node.client_ptr->ok())
            {
                ++node.consumers_cnt;
                clients_queue.push(node);
                clients_status_map[node.client_ptr] = node.consumers_cnt;
                return node.client_ptr;
            }

            clients_status_map.erase(node.client_ptr);
        }
        throw Exception("No available service for " + worker_pool->getServiceName(), ErrorCodes::LOGICAL_ERROR);
    }

    bool KafkaConsumerSchedulerLeastConsumers::shouldReschedule(const CnchWorkerClientPtr current_worker, const String & /* key */, const size_t index)
    {
        /// For each iteration, just update `max_consumers_trigger_reschedule` once by the first consumer
        if (index == 0)
        {
            /// call `initOrUpdateWorkerPool` for updating worker pool in case the vw has scaled up
            initOrUpdateWorkerPool();

            size_t consumers_total_num = 0;
            for (const auto & iter : clients_status_map)
                consumers_total_num += iter.second;

            size_t avg_consumers = (consumers_total_num + clients_status_map.size() - 1) / clients_status_map.size();
            size_t min_extra_consumers = floor(avg_consumers * min_ration_trigger_reschedule);
            max_consumers_trigger_reschedule = avg_consumers + (min_extra_consumers > 0 ? min_extra_consumers : 1);
        }

        auto it = clients_status_map.find(current_worker);
        if (it == clients_status_map.end())
            throw Exception("Cannot find worker while try to reschedule consumer #" + std::to_string(index), ErrorCodes::LOGICAL_ERROR);

        if (it->second > max_consumers_trigger_reschedule)
        {
            resetWorkerClient(current_worker);
            return true;
        }

        return false;
    }

    /// If the consumer failed to start or crashed, we need reset worker client to update queue
    void KafkaConsumerSchedulerLeastConsumers::resetWorkerClient(CnchWorkerClientPtr worker_client)
    {
        if (!worker_client)
            return;

        auto it = clients_status_map.find(worker_client);
        if (it == clients_status_map.end())
        {
            LOG_WARNING(log, "Cannot find worker client {} while trying to reset it", worker_client->getRPCAddress());
            return;
        }

        if (it->second < 1)
        {
            LOG_WARNING(log, "Consumer number on worker {} is 0 while trying to reset it", worker_client->getRPCAddress());
            return;
        }

        --it->second;
        std::priority_queue<client_status> new_clients_queue;
        for (auto & iter : clients_status_map)
            new_clients_queue.emplace(iter.first, iter.second);
        clients_queue = std::move(new_clients_queue);
    }

}
#endif

