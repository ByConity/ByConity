#pragma once
#include <Common/config.h>
#if USE_RDKAFKA

#include <Storages/Kafka/StorageCnchKafka.h>

namespace DB
{
class CnchKafkaOffsetManager
{
public:
    CnchKafkaOffsetManager(ContextPtr context, StorageID & storage_id);
    ~CnchKafkaOffsetManager() = default;

    /*** Reset all offsets for the Kafka table to the earliest offset
     **  whose timestamp is greater than or equal to the given `time_stamp`
     **  See rdkafka.h/rd_kafka_offsets_for_times()
     *
     **  @param time_stamp the Unix timestamp in Milliseconds level
     ** */
    void resetOffsetWithTimestamp(UInt64 time_stamp);

    /*** Reset all offsets to some special value defined by rdkafka, including:
     **    RD_KAFKA_OFFSET_BEGINNING  -2
     *     RD_KAFKA_OFFSET_END        -1
     *     RD_KAFKA_OFFSET_STORED  -1000
     *     RD_KAFKA_OFFSET_INVALID -1001
     * **/
    void resetOffsetToSpecialPosition(int64_t offset);

    /*** Reset some specific offsets which may only have several partitions **/
    void resetOffsetWithSpecificOffsets(const cppkafka::TopicPartitionList & tpl);

private:
    inline bool offsetValueIsSpecialPosition(int64_t value);

    void resetOffsetImpl(const cppkafka::TopicPartitionList & tpl);

    /// Create TopicPartitionList from metadata and update offsets with timestamp (if given)
    cppkafka::TopicPartitionList createTopicPartitionList(uint64_t timestamp);

    ContextPtr global_context;

    StoragePtr storage = nullptr; /// Used to ensure the life cycle
    StorageCnchKafka * kafka_table = nullptr;

    Poco::Logger * log;
};

using CnchKafkaOffsetManagerPtr = std::shared_ptr<CnchKafkaOffsetManager>;

} /// namespace DB

#endif
