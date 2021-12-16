#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastRegistry.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/ExchangeDataKey.h>

#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/tests/gtest_utils.h>
#include <gtest/gtest.h>

namespace UnitTest
{
using namespace DB;

TEST(LocalBroadcast, LocalBroadcastRegistryTest)
{
    LocalChannelOptions options{10, 1000};
    ExchangeDataKey datakey{"", 1, 1, 1, ""};
    BroadcastSenderPtr local_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(datakey, options);
    BroadcastReceiverPtr local_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(datakey, options);
    ASSERT_TRUE(LocalBroadcastRegistry::getInstance().countChannel() == 1);

    local_sender.reset();
    local_receiver.reset();
    ASSERT_TRUE(LocalBroadcastRegistry::getInstance().countChannel() == 0);
}


TEST(LocalBroadcast, NormalSendRecvTest)
{
    LocalChannelOptions options{10, 1000};
    ExchangeDataKey datakey{"", 1, 1, 1, ""};
    BroadcastSenderPtr local_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(datakey, options);
    BroadcastReceiverPtr local_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(datakey, options);
    Chunk chunk = createUInt8Chunk(10, 10, 8);
    auto total_bytes = chunk.bytes();
    BroadcastStatus status = local_sender->send(std::move(chunk));
    ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);

    RecvDataPacket recv_res = local_receiver->recv(100);
    ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
    Chunk & recv_chunk = std::get<Chunk>(recv_res);
    ASSERT_TRUE(recv_chunk.getNumRows() == 10);
    ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
}

TEST(LocalBroadcast, SendTimeoutTest)
{
    LocalChannelOptions options{1, 200};
    ExchangeDataKey datakey{"", 1, 1, 1, ""};
    BroadcastSenderPtr local_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(datakey, options);
    BroadcastReceiverPtr local_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(datakey, options);

    Chunk chunk = createUInt8Chunk(10, 10, 8);
    BroadcastStatus status = local_sender->send(chunk.clone());
    ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    BroadcastStatus timeout_status = local_sender->send(chunk.clone());
    ASSERT_TRUE(timeout_status.code == BroadcastStatusCode::SEND_TIMEOUT);
    ASSERT_TRUE(timeout_status.is_modifer == true);
}

TEST(LocalBroadcast, AllSendDoneTest)
{
    LocalChannelOptions options{10, 1000};
    ExchangeDataKey datakey{"", 1, 1, 1, ""};

    BroadcastSenderPtr local_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(datakey, options);
    BroadcastReceiverPtr local_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(datakey, options);

    Chunk chunk = createUInt8Chunk(10, 10, 8);
    auto total_bytes = chunk.bytes();

    ASSERT_TRUE(local_sender->send(chunk.clone()).code == BroadcastStatusCode::RUNNING);
    ASSERT_TRUE(local_sender->send(chunk.clone()).code == BroadcastStatusCode::RUNNING);
    local_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "Test graceful close");

    ASSERT_TRUE(std::get<Chunk>(local_receiver->recv(100)).bytes() == total_bytes);
    ASSERT_TRUE(std::get<Chunk>(local_receiver->recv(100)).bytes() == total_bytes);

    /// after consume all data, receiver get the ALL_SENDER_DONE status;
    RecvDataPacket res = local_receiver->recv(100);
    ASSERT_TRUE(std::holds_alternative<BroadcastStatus>(res));

    BroadcastStatus & final_status = std::get<BroadcastStatus>(res);
    ASSERT_TRUE(final_status.code == BroadcastStatusCode::ALL_SENDERS_DONE);
    ASSERT_TRUE(final_status.is_modifer == false);
}

}
