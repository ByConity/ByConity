#pragma once

#include <Common/config.h>
#if USE_RDKAFKA

#include <Core/Types.h>
#include <Interpreters/StorageID.h>

#include <cppkafka/cppkafka.h>

namespace DB
{
struct KafkaTaskCommand
{
    enum Type
    {
        UNKNOWN_TYPE,
        START_CONSUME,
        STOP_CONSUME
    };

    Type type{UNKNOWN_TYPE};

    String task_id;
    UInt16 rpc_port{0};

    /// database name and table name of StorageCnchKafka
    StorageID cnch_storage_id{StorageID::createEmpty()};
    String local_database_name;
    String local_table_name;

    /// assigned by task-manager and send to worker to allocate StorageCnchKafkaConsumer
    size_t assigned_consumer;

    /// commands for creating all three tables
    /// material-view table comes the last
    std::vector<String> create_table_commands;

    /// partition-list of topics for each consumer
    cppkafka::TopicPartitionList tpl;

    static const char * typeToString(Type type)
    {
        switch (type)
        {
            case Type::START_CONSUME:
                return "START CONSUME";
            case Type::STOP_CONSUME:
                return "STOP CONSUME";
            default:
                return "UNKNOWN TYPE";
        }
    }
};

struct CnchConsumerStatus
{
    String cluster;
    Strings topics;
    Strings assignment;
    UInt32 assigned_consumers;
    String last_exception;
};

struct KafkaTableInfo
{
    String database;
    String table;
    String uuid;
    String cluster;
    Strings topics;
    String consumer_group;
};

struct KafkaConsumerRunningInfo
{
    bool is_running;
    String table_suffix;
    String worker_client_info;
    cppkafka::TopicPartitionList partitions;
};

} // namespace DB

#endif

