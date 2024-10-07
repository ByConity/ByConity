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
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <boost/noncopyable.hpp>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include <common/types.h>

namespace DB
{
class BroadcastSenderProxy;
struct SenderProxyOptions;

class BroadcastSenderProxyRegistry final : private boost::noncopyable
{
public:
    static BroadcastSenderProxyRegistry & instance()
    {
        static BroadcastSenderProxyRegistry * instance = new BroadcastSenderProxyRegistry;
        return *instance;
    }

    BroadcastSenderProxyPtr get(ExchangeDataKeyPtr data_key);

    BroadcastSenderProxyPtr getOrCreate(ExchangeDataKeyPtr data_key);

    BroadcastSenderProxyPtr getOrCreate(ExchangeDataKeyPtr data_key, SenderProxyOptions options);

    void remove(ExchangeDataKeyPtr data_key);

    size_t countProxies();

private:
    BroadcastSenderProxyRegistry();
    mutable bthread::Mutex mutex;
    using BroadcastSenderProxyEntry = std::weak_ptr<BroadcastSenderProxy>;
    std::unordered_map<ExchangeDataKey, BroadcastSenderProxyEntry, ExchangeDataKeyHashFunc> proxies;
    LoggerPtr logger;
};

}
