#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CURDIR/../shell_config.sh

# We only test Bytedance-Kafka now
use_bytedance_kafka=`$CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name='USE_BYTEDANCE_RDKAFKA'"`
if [ "${use_bytedance_kafka}"x != "ON"x ] && [ "${use_bytedance_kafka}"x != "1"x ]; then
  echo -e "1\n1\n2\n1\nOffsets committed\n1\nMul sink works"
  exit 0
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test.kafka_mv_v3"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test.kafka_store_v3"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test.kafka_mv_v2"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test.kafka_store_v2"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test.kafka_mv"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test.kafka_consumer"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test.kafka_store"

# Create cnch-kafka tables tuple with unique consumer group to avoid conflict among multi-pipelines
timestamp=$(date +%s%3N)
consumer_group="cnch_kafka_ci_test_"$timestamp
$CLICKHOUSE_CLIENT --multiquery <<'EOF'
CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.kafka_mv_v3;
DROP TABLE IF EXISTS test.kafka_store_v3;
DROP TABLE IF EXISTS test.kafka_mv_v2;
DROP TABLE IF EXISTS test.kafka_store_v2;
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
# 1. Check running consumer number, result should be '1'
$CLICKHOUSE_CLIENT --query "SELECT num_consumers FROM system.cnch_kafka_tables WHERE database = 'test' AND name = 'kafka_consumer'"

# Consuming: greater than twice of flush_interval_milliseconds for kafka_log to ensure read kafka_log
sleep 21

# 2. Check consumption result, result should be '1'
$CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM test.kafka_store"

# Alter kafka table (TODO: support forward Alter query to target server)
$CLICKHOUSE_CLIENT --query "ALTER TABLE test.kafka_consumer MODIFY SETTING kafka_num_consumers = 2"
sleep 3
# 3. Check running consumer number after ALTER, result should be '2'
$CLICKHOUSE_CLIENT --query "SELECT num_consumers FROM system.cnch_kafka_tables WHERE database = 'test' AND name = 'kafka_consumer'"

# 4. Check cnch_system.cnch_kafka_log, result should be '1'
$CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM cnch_system.cnch_kafka_log WHERE cnch_database = 'test' AND cnch_table = 'kafka_consumer' AND event_type='PARSE_ERROR'"

# Check offset (TODO: implement it after supporting system.cnch_kafka_tables)
assign=`$CLICKHOUSE_CLIENT --query "SELECT consumer_partitions FROM system.cnch_kafka_tables WHERE database = 'test' AND name = 'kafka_consumer'"`
offset=`$CLICKHOUSE_CLIENT --query "SELECT consumer_offsets FROM system.cnch_kafka_tables WHERE database = 'test' AND name = 'kafka_consumer'"`
if [ "$assign"x = "$offset"x ]; then
   echo "No offsets committed"
else
   echo "Offsets committed"
fi

#END Consume
$CLICKHOUSE_CLIENT --query "SYSTEM STOP CONSUME test.kafka_consumer"

# Test mul sink tables
$CLICKHOUSE_CLIENT --multiquery <<'EOF'
CREATE TABLE test.kafka_store_v2(
    `i` Int64,
    `ts` DateTime
) ENGINE = CnchMergeTree
PARTITION BY toDate(ts) ORDER BY ts;

CREATE MATERIALIZED VIEW test.kafka_mv_v2 TO test.kafka_store_v2 AS SELECT i, ts FROM test.kafka_store;

CREATE TABLE test.kafka_store_v3(
    `i` Int64,
    `ts` DateTime
) ENGINE = CnchMergeTree
PARTITION BY toDate(ts) ORDER BY ts;

CREATE MATERIALIZED VIEW test.kafka_mv_v3 TO test.kafka_store_v3 AS SELECT i, ts FROM test.kafka_store_v2;

SYSTEM START CONSUME test.kafka_consumer;
EOF

# Consuming: greater than twice of flush_interval_milliseconds
sleep 21

# 2. Check consumption result, result should be '1'
$CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM test.kafka_store_v2"

#END Consume
$CLICKHOUSE_CLIENT --query "SYSTEM STOP CONSUME test.kafka_consumer"
sleep 2

# Check count
count1=`$CLICKHOUSE_CLIENT --query "SELECT count() FROM test.kafka_store_v2"`
count2=`$CLICKHOUSE_CLIENT --query "SELECT count() FROM test.kafka_store_v3"`
if [ "$count1"x = "$count2"x ]; then
   echo "Mul sink works"
else
   echo "Mul sink fails"
fi

#END
$CLICKHOUSE_CLIENT --query "DROP TABLE test.kafka_mv_v3"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.kafka_store_v3"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.kafka_mv_v2"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.kafka_store_v2"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.kafka_mv"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.kafka_consumer"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.kafka_store"
