#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="CREATE DATABASE IF NOT EXISTS test;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.test_direct_insert;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.test_direct_insert(x UInt64, id UInt64) Engine=CnchMergeTree order by id;"
${CNCH_WRITE_WORKER_CLIENT} -n --query="SET prefer_cnch_catalog=1; INSERT INTO test.test_direct_insert VALUES (1, 1);"
${CNCH_WRITE_WORKER_CLIENT} -n --query="INSERT INTO test.test_direct_insert FORMAT Values SETTINGS prefer_cnch_catalog=1 (2, 2);"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.test_direct_insert;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.test_direct_insert;"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.events_aggregation;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.events_aggregate_view;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.events;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.events(app_id UInt32, server_time UInt64, event_name String, uid UInt64, cost UInt64, duration UInt64, event_date Date) ENGINE = CnchMergeTree PARTITION BY toDate(event_date) ORDER BY (app_id, uid, event_name);"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.events_aggregation (app_id UInt32,  event_name String, event_date Date, sum_cost AggregateFunction(sum, UInt64) , max_duration AggregateFunction(max, UInt64)) ENGINE = CnchAggregatingMergeTree() PARTITION BY toDate(event_date) ORDER BY (app_id, event_name, event_date);"
${CLICKHOUSE_CLIENT} --query="CREATE MATERIALIZED VIEW test.events_aggregate_view to test.events_aggregation(app_id UInt32, event_name String, event_date Date, sum_cost AggregateFunction(sum, UInt64), max_duration AggregateFunction(max, UInt64)) AS SELECT app_id, event_name, event_date, sumState(cost) AS sum_cost, maxState(duration) AS max_duration FROM test.events GROUP BY app_id, event_name, event_date;"
${CNCH_WRITE_WORKER_CLIENT} -n --query="SET prefer_cnch_catalog=1; insert into table test.events values (1, 87657, 'show', 121245, 1, 64, '2022-06-14'),(2, 252234 , 'send', 2345, 2, 64, '2022-06-14');"
${CLICKHOUSE_CLIENT} --query="insert into table test.events values (3, 234343, 'show', 121245, 3, 64, '2022-06-15'),(4, 234343, 'show', 121245, 3, 64, '2022-06-15');"
${CLICKHOUSE_CLIENT} --query="explain SELECT app_id, event_name, event_date, sum(cost) AS sum_cost, max(duration) AS max_duration FROM test.events GROUP BY app_id, event_name, event_date settings enable_optimizer = 1;"
${CLICKHOUSE_CLIENT} --query="SELECT app_id, event_name, event_date, sum(cost) AS sum_cost, max(duration) AS max_duration FROM test.events GROUP BY app_id, event_name, event_date order by app_id settings enable_optimizer = 1;"
${CLICKHOUSE_CLIENT} --query="SELECT app_id, event_name, event_date, sum(cost) AS sum_cost, max(duration) AS max_duration FROM test.events GROUP BY app_id, event_name, event_date order by app_id settings enable_optimizer = 0;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.events_aggregation;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.events_aggregate_view;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.events;"

