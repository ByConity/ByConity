#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CURDIR/../shell_config.sh

# We only test Bytedance-Kafka now
use_bytedance_kafka=`$CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name='USE_BYTEDANCE_RDKAFKA'"`
if [ "${use_bytedance_kafka}"x != "ON"x ] && [ "${use_bytedance_kafka}"x != "1"x ]; then
  #echo -e "1\n1\n1\nOffsets committed\n2\nselect on running table\ntest	kafka_consumer	2\ntest	kafka_consumer	2\n2\n0\n0\n1\n1\nselect on stopped table\n0\n1\n0\ntest	kafka_consumer	0"
  echo -e "1\n1"
  exit 0
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test.kafka_mv"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test.kafka_consumer"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test.kafka_store"

# Create cnch-kafka tables tuple with unique consumer group to avoid conflict among multi-pipelines
timestamp=$(date +%s%3N)
consumer_group="cnch_kafka_ci_test_"$timestamp
$CLICKHOUSE_CLIENT --multiquery <<'EOF'
CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.kafka_mv;
DROP TABLE IF EXISTS test.kafka_consumer;
DROP TABLE IF EXISTS test.kafka_store;
EOF

$CLICKHOUSE_CLIENT --query "CREATE TABLE test.kafka_consumer ( i Int64, ts DateTime \
) ENGINE = CnchKafka() SETTINGS \
kafka_cluster = 'bmq_test_lq', \
kafka_topic_list = 'cnch_kafka_ci_test_topic', \
kafka_group_name = "\'$consumer_group\'", \
kafka_format = 'JSONEachRow', \
kafka_row_delimiter = '\n', \
kafka_num_consumers = 1;"

$CLICKHOUSE_CLIENT --multiquery <<'EOF'
CREATE TABLE test.kafka_store(
    `i` Int64,
    `ts` DateTime
) ENGINE = CnchMergeTree
PARTITION BY toDate(ts) ORDER BY ts;

CREATE MATERIALIZED VIEW test.kafka_mv TO test.kafka_store AS SELECT i, ts FROM test.kafka_consumer;

SYSTEM START CONSUME test.kafka_consumer;
EOF

# Start consume
start_consume_time=$(date +%s%3N)
sleep 2
$CLICKHOUSE_CLIENT --query "SELECT num_consumers FROM system.kafka_tables WHERE database = 'test' AND name = 'kafka_consumer'"

# Consuming: twice of flush_interval_milliseconds for kafka_log to ensure read kafka_log
sleep 21

# Check consumption result (TODO: cnch_system.cnch_kafka_log is not implemented now)
$CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM test.kafka_store"
#$CNCH_WRITE_WORKER_CLIENT --query "SELECT count() > 0 FROM system.kafka_log WHERE cnch_database = 'test' AND cnch_table = 'kafka_consumer' AND event_type='PARSE_ERROR'"

# Check offset (TODO: implement it after supporting system.cnch_kafka_tables)
#assign=`$CLICKHOUSE_CLIENT --query "SELECT consumer_partitions FROM system.kafka_tables WHERE database = 'test' AND name = 'kafka_consumer'"`
#offset=`$CLICKHOUSE_CLIENT --query "SELECT consumer_offsets FROM system.kafka_tables WHERE database = 'test' AND name = 'kafka_consumer'"`
#if [ "$assign"x = "$offset"x ]; then
#    echo "No offsets committed"
#else
#    echo "Offsets committed"
#fi

# Alter kafka table (TODO: support forward Alter query to target server)
#$CLICKHOUSE_CLIENT --query "ALTER TABLE test.kafka_consumer MODIFY SETTING kafka_num_consumers = 2"
#sleep 2
#$CLICKHOUSE_CLIENT --query "SELECT num_consumers FROM system.kafka_tables WHERE database = 'test' AND name = 'kafka_consumer'"

#END
$CLICKHOUSE_CLIENT --query "SYSTEM STOP CONSUME test.kafka_consumer"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.kafka_mv"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.kafka_consumer"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.kafka_store"