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

#include <memory>
#include <mutex>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <common/logger_useful.h>

namespace DB
{
BroadcastSenderProxyRegistry::BroadcastSenderProxyRegistry() : logger(getLogger("BroadcastSenderProxyRegistry"))
{
}

BroadcastSenderProxyPtr BroadcastSenderProxyRegistry::get(ExchangeDataKeyPtr data_key)
{
    std::lock_guard lock(mutex);
    auto it = proxies.find(*data_key);
    if (it != proxies.end())
    {
        auto channel_ptr = it->second.lock();
        if (channel_ptr)
            return channel_ptr;
    }
    return nullptr;
}

BroadcastSenderProxyPtr BroadcastSenderProxyRegistry::getOrCreate(ExchangeDataKeyPtr data_key)
{
    return getOrCreate(std::move(data_key), SenderProxyOptions{.wait_timeout_ms = 5000});
}

BroadcastSenderProxyPtr BroadcastSenderProxyRegistry::getOrCreate(ExchangeDataKeyPtr data_key, SenderProxyOptions options)
{
    std::lock_guard lock(mutex);
    auto it = proxies.find(*data_key);
    if (it != proxies.end())
    {
        auto channel_ptr = it->second.lock();
        if (channel_ptr)
            return channel_ptr;
    }

    LOG_TRACE(logger, "Register sender proxy with key {}", *data_key);
    auto channel_ptr = std::shared_ptr<BroadcastSenderProxy>(new BroadcastSenderProxy(std::move(data_key), std::move(options)));
    proxies.emplace(*channel_ptr->getDataKey(), BroadcastSenderProxyEntry(channel_ptr));
    return channel_ptr;
}

void BroadcastSenderProxyRegistry::remove(ExchangeDataKeyPtr data_key)
{
    std::lock_guard lock(mutex);
    auto result = proxies.erase(*data_key);
    LOG_TRACE(logger, "remove proxy {} with result: {} ", *data_key, result);
}

size_t BroadcastSenderProxyRegistry::countProxies()
{
    std::lock_guard lock(mutex);
    return proxies.size();
}

}
