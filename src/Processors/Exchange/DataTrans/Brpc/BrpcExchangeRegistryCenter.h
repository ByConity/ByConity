#pragma once

#include <Interpreters/Context.h>
#include <Processors/Exchange/DataTrans/ConcurrentShardMap.h>
#include <brpc/stream.h>

namespace DB
{
class BrpcExchangeRegistryCenter final : private boost::noncopyable
{
public:
    static BrpcExchangeRegistryCenter & getInstance()
    {
        static BrpcExchangeRegistryCenter ret;
        return ret;
    }

    void submit(const String & receiver_id_, const brpc::StreamId stream_id_) { receiver_id_to_sender.put(receiver_id_, stream_id_); }

    void removeReceiver(const String & receiver_id_) { receiver_id_to_sender.remove(receiver_id_); }

    bool exist(const String & receiver_id_) { return receiver_id_to_sender.exist(receiver_id_); }

    size_t size() { return receiver_id_to_sender.size(); }

    brpc::StreamId getSenderStreamId(const String & receiver_id_) { return receiver_id_to_sender.get(receiver_id_); }

private:
    Poco::Logger * log = &Poco::Logger::get("BrpcExchangeRegistryCenter");
    ConcurrentShardMap<String, brpc::StreamId> receiver_id_to_sender;
};
}
