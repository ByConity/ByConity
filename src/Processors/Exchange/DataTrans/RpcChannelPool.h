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
#include <thread>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <bthread/shared_mutex.h>
#include <parallel_hashmap/phmap.h>
#include <Poco/DateTimeFormatter.h>
#include <Common/Brpc/BrpcApplication.h>
#include <Common/Brpc/BrpcChannelPoolConfigHolder.h>
#include <common/logger_useful.h>

namespace DB
{

///  Singleton class, use std::cout for log
class RpcChannelPool : public boost::noncopyable
{
private:
    struct Pool
    {
        std::vector<std::shared_ptr<RpcClient>> clients;
        std::atomic<UInt32> counter{0};
        std::atomic<time_t> recent_used_ts{0};
        std::atomic_bool ok_{true};

        Pool() = default;
        explicit Pool(std::vector<std::shared_ptr<RpcClient>>::size_type n)
        {
            this->clients = std::vector<std::shared_ptr<RpcClient>>(n, nullptr);
            updateRecentUsedTime();
        }

        time_t getRecentUsedTime() const { return recent_used_ts;}
        inline void updateRecentUsedTime() { recent_used_ts = time(nullptr); }
    
        bool ok() const { return ok_.load(std::memory_order_relaxed); }
    };
    using PoolOptionsMap = BrpcChannelPoolConfigHolder::PoolOptionsMap;
    using ClientType = std::string;
    using HostPort = std::string;
    using PoolPtr = std::shared_ptr<Pool>;

public:
    static RpcChannelPool & getInstance()
    {
        static RpcChannelPool pool;
        return pool;
    }

    std::shared_ptr<RpcClient> getClient(const String & host_port, const std::string & client_type, bool refresh = false);

    brpc::ChannelOptions getChannelPoolOptions(const std::string & client_type)
    {
        auto iter = channel_pool.find(client_type);
        if (unlikely(iter == channel_pool.end()))
            iter = channel_pool.find(BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);

        auto pool_options = std::atomic_load(&iter->second.pool_options);
        return pool_options.get()->channel_options;
    }

    void initPoolExpireTimer(UInt64 rpc_channel_pool_check_interval_seconds_ = 0, UInt64 rpc_channel_pool_expired_seconds_ = 0)
    {
        static std::once_flag flag;
        if (rpc_channel_pool_expired_seconds_ != 0 && rpc_channel_pool_check_interval_seconds_ != 0)
        {
            rpc_channel_pool_expired_seconds = rpc_channel_pool_expired_seconds_;
            rpc_channel_pool_check_interval_seconds = rpc_channel_pool_check_interval_seconds_;
            std::call_once(flag, [this]() { this->createExpireTimer(); });
            LOG_DEBUG(
                log,
                "channel pool check interval config changed [{}, {}].",
                rpc_channel_pool_expired_seconds,
                rpc_channel_pool_check_interval_seconds);
        }
    }

    void destroyExpireTimer();

private:
    std::shared_ptr<BrpcChannelPoolConfigHolder> channel_pool_config_holder;

    using PoolOptionsPtr = std::shared_ptr<BrpcChannelPoolOptions::PoolOptions>;
    using Container = phmap::parallel_flat_hash_map<
        HostPort,
        PoolPtr,
        phmap::priv::hash_default_hash<HostPort>,
        phmap::priv::hash_default_eq<HostPort>,
        phmap::priv::Allocator<phmap::priv::Pair<HostPort, PoolPtr>>,
        4,
        bthread::Mutex>;
    struct PoolContent
    {
        PoolOptionsPtr pool_options;
        Container host_port_pool;
    };
    std::unordered_map<ClientType, PoolContent> channel_pool;

    std::atomic<uint64_t> rpc_channel_pool_check_interval_seconds{0};
    std::atomic<uint64_t> rpc_channel_pool_expired_seconds{0};
    std::unique_ptr<std::thread> expireThread{nullptr};
    bthread::Mutex mutex;
    bthread::ConditionVariable cv;
    std::atomic_bool exit{false};

    LoggerPtr log = getLogger("RpcChannelPool");

    void createExpireTimer();
    size_t checkAndClearExpiredPool(const std::string & client_type);
    inline PoolOptionsMap getChannelPoolOptions() { return channel_pool_config_holder->queryConfig(); }

    RpcChannelPool()
    {
        channel_pool_config_holder = BrpcApplication::getInstance().getConfigHolderByType<BrpcChannelPoolConfigHolder>();

        auto pool_options_map = getChannelPoolOptions();
        for (const auto & pair : pool_options_map)
        {
            const auto & key = pair.first;
            channel_pool[key].pool_options = std::make_shared<BrpcChannelPoolOptions::PoolOptions>(pair.second);
        }

        // can't init channel_pool here

        // lock in the granularity of client_type if the options change after reload
        auto channel_pool_reload_callback = [this](const PoolOptionsMap * old_conf_to_del, const PoolOptionsMap * readonly_new_conf) {
            // normally, there should be NO insert or delete in pool options
            if (old_conf_to_del->size() > readonly_new_conf->size())
            {
                std::cout << Poco::DateTimeFormatter::format(Poco::DateTime(), "%Y.%m.%d %H:%M:%S.%i") << " <Error> "
                          << "RpcChannelPool::RpcChannelPool "
                          << "Existing pool options should NOT be deleted! Reload callback suspend.";
                return;
            }
            else if (old_conf_to_del->size() < readonly_new_conf->size())
            {
                std::cout << Poco::DateTimeFormatter::format(Poco::DateTime(), "%Y.%m.%d %H:%M:%S.%i") << " <Warning> "
                          << "RpcChannelPool::RpcChannelPool "
                          << "Existing pool options should NOT be added. Added ones will be omitted.";
            }

            for (const auto & pair : *old_conf_to_del)
            {
                const auto & key = pair.first;
                auto itr = readonly_new_conf->find(key);
                if (itr == readonly_new_conf->end())
                {
                    std::cout << Poco::DateTimeFormatter::format(Poco::DateTime(), "%Y.%m.%d %H:%M:%S.%i") << " <Error> "
                              << "RpcChannelPool::RpcChannelPool "
                              << "Existing pool options should NOT be deleted! Reload callback suspend, but some might have updated.";
                    return;
                }
                if (itr->second != pair.second) // update
                {
                    auto pool_options = std::make_shared<BrpcChannelPoolOptions::PoolOptions>(itr->second);
                    std::atomic_store(&channel_pool[key].pool_options, std::move(pool_options));
                    channel_pool[key].host_port_pool.clear();
                }
            }
        };
        channel_pool_config_holder->initReloadCallback(channel_pool_reload_callback);        
    }

    ~RpcChannelPool()
    {
        destroyExpireTimer();
    }
};

}
