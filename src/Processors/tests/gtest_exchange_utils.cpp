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
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <brpc/stream.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastSender.h>
#include <parallel_hashmap/phmap.h>
#include <unordered_map>

using namespace DB;
namespace UnitTest
{
TEST(ExchangeUtils, mergeSenderTest)
{
    initLogger();
    const auto & context = getContext().context;
    Block header;
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 1000 * 1000000;
    LocalChannelOptions options{context->getSettingsRef().exchange_local_receiver_queue_size, ts, false};
    auto data_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto channel = std::make_shared<LocalBroadcastChannel>(data_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr local_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    local_sender->becomeRealSender(channel);

    BroadcastSenderPtrs senders;
    senders.emplace_back(std::move(local_sender));

    for (size_t i = 0; i < 2; i++)
    {
        auto bprc_data_key = std::make_shared<ExchangeDataKey>(1, 2, i);
        BroadcastSenderProxyPtr sender_proxy = BroadcastSenderProxyRegistry::instance().getOrCreate(bprc_data_key);
        sender_proxy->accept(context, header);
        auto brpc_sender = std::make_shared<BrpcRemoteBroadcastSender>(bprc_data_key, brpc::INVALID_STREAM_ID ,context, header);
        sender_proxy->becomeRealSender(std::move(brpc_sender));
        senders.emplace_back(std::move(sender_proxy));
    }

    ExchangeUtils::mergeSenders(senders);
    ASSERT_TRUE(senders.size() == 2);
}

TEST(ExchangeUtils, parallelHashmapTest)
{
    typedef phmap::parallel_flat_hash_map<int, int,
		phmap::priv::hash_default_hash<int>,
		phmap::priv::hash_default_eq<int>,
		std::allocator<std::pair<int, int>>,
		4, bthread::Mutex> Map;

    Map m = { {1, 7}, {2, 9} };
    m.lazy_emplace_l(5, 
                     [](Map::value_type& v) { v.second = 6; },           // called only when key was already present
                     [](const Map::constructor& ctor) { ctor(5, 13); }); // construct value_type in place when key not present 
    ASSERT_TRUE(m[5] == 13);

    // change a value that is present. Currently m[5] == 13
    m.lazy_emplace_l(5, 
                     [](Map::value_type& v) { v.second = 6; },           // called only when key was already present
                     [](const Map::constructor& ctor) { ctor(5, 13); }); // construct value_type in place when key not present
    ASSERT_TRUE(m[5] == 6);
}

}
