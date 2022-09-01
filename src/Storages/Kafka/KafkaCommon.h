#pragma once

#include <Common/StringUtils/StringUtils.h>
#include <Core/Names.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <cppkafka/configuration.h>
#include <cppkafka/topic_partition_list.h>

namespace DB::Kafka
{
    const String HDFS_PREFIX = "hdfs://";
    const String CFS_PREFIX = "cfs://";

    inline bool startsWithHDFSOrCFS(const String& name)
    {
        return startsWith(name, HDFS_PREFIX) || startsWith(name, CFS_PREFIX);
    }

cppkafka::Configuration createConsumerConfiguration(
        ContextPtr context, const StorageID & storage_id, const Names & topics, const KafkaSettings & settings);

String toString(const cppkafka::TopicPartitionList & tpl);
}
