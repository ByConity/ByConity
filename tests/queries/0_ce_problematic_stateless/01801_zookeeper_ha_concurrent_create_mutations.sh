#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test: mutation with bigger mutation_id should have bigger block number
# ----
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS concurrent_ha_mut"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE concurrent_ha_mut(d Date, x UInt32, s String) ENGINE HaMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/concurrent_ha_mut', 'r1') PARTITION BY d ORDER BY x"
for i in `seq 5`
do
  # simulate concurrent adding mutations by executing the command in the background
  ${CLICKHOUSE_CLIENT} --query="alter table concurrent_ha_mut fastdelete x where x = 0 SETTINGS mutations_sync=1" 2>/dev/null &
done
wait # wait for previous alter statements to finish
${CLICKHOUSE_CLIENT} --query="select mutation_id, is_done from system.mutations where database='$CLICKHOUSE_DATABASE' and table='concurrent_ha_mut' order by block_numbers.number"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS concurrent_ha_mut"

# Test: we won't create multiple mutations with the same query id
# ----
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_dedup_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_dedup_r2"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ha_mut_dedup_r1(d Date, x UInt32, s String) ENGINE HaMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ha_mut_dedup', 'r1') PARTITION BY d ORDER BY x"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ha_mut_dedup_r2(d Date, x UInt32, s String) ENGINE HaMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ha_mut_dedup', 'r2') PARTITION BY d ORDER BY x"

${CLICKHOUSE_CLIENT} --query="alter table ha_mut_dedup_r1 fastdelete x where x = 0 SETTINGS mutation_query_id='my_id'" 2>/dev/null &
${CLICKHOUSE_CLIENT} --query="alter table ha_mut_dedup_r2 fastdelete x where x = 0 SETTINGS mutation_query_id='my_id'" 2>/dev/null &
${CLICKHOUSE_CLIENT} --query="alter table ha_mut_dedup_r1 fastdelete x where x = 0 SETTINGS mutation_query_id='my_id'" 2>/dev/null &
${CLICKHOUSE_CLIENT} --query="alter table ha_mut_dedup_r2 fastdelete x where x = 0 SETTINGS mutation_query_id='my_id'" 2>/dev/null &
wait
echo 'after create mutations with the same query id'

${CLICKHOUSE_CLIENT} --query="system sync mutation ha_mut_dedup_r1"
${CLICKHOUSE_CLIENT} --query="select 'r1', mutation_id, is_done from system.mutations where database='$CLICKHOUSE_DATABASE' and table='ha_mut_dedup_r1' and query_id='my_id'"
${CLICKHOUSE_CLIENT} --query="system sync mutation ha_mut_dedup_r2"
${CLICKHOUSE_CLIENT} --query="select 'r2', mutation_id, is_done from system.mutations where database='$CLICKHOUSE_DATABASE' and table='ha_mut_dedup_r2' and query_id='my_id'"

${CLICKHOUSE_CLIENT} --query="alter table ha_mut_dedup_r1 fastdelete x where x = 1 SETTINGS mutation_query_id='my_id'" 2>/dev/null \
  || echo "Create mutation with same query id but different commands should fail"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_dedup_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_dedup_r2"

# Test: mutation cleaner
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_cleaner_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_cleaner_r2"

# Create 2 replicas with finished_mutations_to_keep = 2
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ha_mut_cleaner_r1(x UInt32) ENGINE HaMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ha_mut_cleaner', 'r1') ORDER BY x SETTINGS \
    finished_mutations_to_keep = 2,
    cleanup_delay_period = 1,
    cleanup_delay_period_random_add = 0"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ha_mut_cleaner_r2(x UInt32) ENGINE HaMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ha_mut_cleaner', 'r2') ORDER BY x SETTINGS \
    finished_mutations_to_keep = 2,
    cleanup_delay_period = 1,
    cleanup_delay_period_random_add = 0"

# Insert some data
${CLICKHOUSE_CLIENT} --query="INSERT INTO ha_mut_cleaner_r1(x) VALUES (1), (2), (3), (4)"
${CLICKHOUSE_CLIENT} --query="system sync replica ha_mut_cleaner_r2"

# Add some mutations and wait for their execution
${CLICKHOUSE_CLIENT} --query="ALTER TABLE ha_mut_cleaner_r1 DELETE WHERE x = 1 SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE ha_mut_cleaner_r1 DELETE WHERE x = 2 SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE ha_mut_cleaner_r1 DELETE WHERE x = 3 SETTINGS mutations_sync = 2"

# Add another mutation and prevent its execution on the second replica
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP REPLICATION QUEUES ha_mut_cleaner_r2"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE ha_mut_cleaner_r1 DELETE WHERE x = 4 SETTINGS mutations_sync = 1"

# Sleep for more than cleanup_delay_period
sleep 3

# Check that the first mutation is cleaned
${CLICKHOUSE_CLIENT} --query="SELECT mutation_id, command, is_done FROM system.mutations WHERE database='$CLICKHOUSE_DATABASE' and table='ha_mut_cleaner_r2' ORDER BY mutation_id"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_cleaner_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_cleaner_r2"
