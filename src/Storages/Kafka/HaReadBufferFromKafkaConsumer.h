#pragma once
#include <Common/config.h>
#if USE_RDKAFKA

#include <Core/Names.h>
#include <Core/Types.h>
#include <IO/DelimitedReadBuffer.h>
#include <common/logger_useful.h>

#include <cppkafka/cppkafka.h>
#include <Storages/Kafka/KafkaConsumer.h>

namespace DB
{

using BufferPtr = std::shared_ptr<DelimitedReadBuffer>;
using ConsumerPtr = std::shared_ptr<KafkaConsumer>;

struct pair_hash
{
    template <class T1, class T2>
    std::size_t operator() (const std::pair<T1, T2> & p) const
    {
        static std::hash<T1> t1;
        static std::hash<T2> t2;
        return t1(p.first) ^ t2(p.second);
    }
};

class HaReadBufferFromKafkaConsumer : public ReadBuffer
{
    using Message = cppkafka::Message;

public:
    HaReadBufferFromKafkaConsumer(
        ConsumerPtr consumer_,
        const String & logger_name,
        size_t max_batch_size,
        size_t poll_timeout_,
        size_t expire_timeout_,
        std::atomic_bool *run_)
        : ReadBuffer(nullptr, 0)
        , consumer(consumer_)
        , log(&Poco::Logger::get(logger_name))
        , batch_size(max_batch_size)
        , poll_timeout(poll_timeout_)
        , expire_timeout(expire_timeout_)
        , run(run_)
        , create_time(time(nullptr))
    {
    }

    ~HaReadBufferFromKafkaConsumer() override;

    void commit(); // Commit all processed messages.
    void subscribe(const Names & topics); // Subscribe internal consumer to topics.
    void unsubscribe(); // Unsubscribe internal consumer in case of failure.
    void assign(const cppkafka::TopicPartitionList & topic_partition_list);
    void unassign();

    auto pollTimeout() const { return poll_timeout; }

    ConsumerPtr & getConsumer() { return consumer; }

    void reset();

    cppkafka::TopicPartitionList getOffsets() const;

    size_t getReadMessages() const { return read_messages; }
    size_t getReadBytes() const { return read_bytes; }
    size_t getEmptyMessages() const { return empty_messages; }
    size_t getCreateTime() const { return create_time; }
    size_t getAliveTime() const { return alive_time; }

    // Return values for the message that's being read.
    const Message & currentMessage() const { return current; }
    String currentTopic() const { return current.get_topic(); }
    String currentKey() const { return current.get_key(); }
    auto currentOffset() const { return current.get_offset(); }
    auto currentPartition() const {return current.get_partition();}
    String currentContent() const {return current.get_payload();}
    UInt64 currentTimeStamp() const
    {
        auto ts = current.get_timestamp();
        if (ts)
            return ts->get_timestamp().count();
        else
            return 0;
    }

private:
    ConsumerPtr consumer;
    Poco::Logger * log;
    size_t batch_size;
    size_t poll_timeout;
    size_t expire_timeout;
    std::atomic_bool * run;

    size_t create_time;
    size_t alive_time {0};
    bool stalled = false;

    std::mutex cancel_mutex;
    std::condition_variable cancel_cv;
    std::atomic_bool cancelled{false};

    Message current;

    size_t read_messages {0};
    size_t empty_messages {0};
    size_t read_bytes {0};

    std::unordered_map<
        std::pair<std::string, std::uint64_t>,
        std::int64_t,
        pair_hash> offsets;

    bool nextImpl() override;
    bool hasExpired();
};

}

#endif
