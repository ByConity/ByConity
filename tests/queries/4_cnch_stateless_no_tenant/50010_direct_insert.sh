#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.test_direct_insert;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.test_direct_insert(x UInt64, id UInt64) Engine=CnchMergeTree order by id;"
${CNCH_WRITE_WORKER_CLIENT} -n --query="SET prefer_cnch_catalog=1; INSERT INTO test.test_direct_insert VALUES (1, 1);"
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
${CLICKHOUSE_CLIENT} --query="explain SELECT app_id, event_name, event_date, sum(cost) AS sum_cost, max(duration) AS max_duration FROM test.events GROUP BY app_id, event_name, event_date order by app_id settings enable_optimizer = 1;"
${CLICKHOUSE_CLIENT} --query="SELECT app_id, event_name, event_date, sum(cost) AS sum_cost, max(duration) AS max_duration FROM test.events GROUP BY app_id, event_name, event_date order by app_id settings enable_optimizer = 1;"
${CLICKHOUSE_CLIENT} --query="SELECT app_id, event_name, event_date, sum(cost) AS sum_cost, max(duration) AS max_duration FROM test.events GROUP BY app_id, event_name, event_date order by app_id settings enable_optimizer = 0;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.events_aggregation;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.events_aggregate_view;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.events;"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.wikistat_daily_summary_mv;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.wikistat_daily_summary_mv_2;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.wikistat_daily_summary;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.wikistat;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.wikistat(time DateTime CODEC(Delta(4), ZSTD(1)), project LowCardinality(String), subproject LowCardinality(String), path String, hits UInt64) ENGINE = CnchMergeTree ORDER BY (path, time) SETTINGS index_granularity = 8192;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.wikistat_daily_summary(project String, date Date, min_hits_per_hour AggregateFunction(min, UInt64), max_hits_per_hour AggregateFunction(max, UInt64), avg_hits_per_hour AggregateFunction(avg, UInt64)) ENGINE = CnchAggregatingMergeTree ORDER BY (project, date) SETTINGS index_granularity = 8192;"
${CLICKHOUSE_CLIENT} --query="CREATE MATERIALIZED VIEW test.wikistat_daily_summary_mv TO test.wikistat_daily_summary( project LowCardinality(String), date Date, min_hits_per_hour AggregateFunction(min, UInt64), max_hits_per_hour AggregateFunction(max, UInt64), avg_hits_per_hour AggregateFunction(avg, UInt64)) AS SELECT project, toDate(time) AS date, minState(hits) AS min_hits_per_hour, maxState(hits) AS max_hits_per_hour, avgState(hits) AS avg_hits_per_hour FROM test.wikistat GROUP BY project, date;"
${CLICKHOUSE_CLIENT} --query="CREATE MATERIALIZED VIEW test.wikistat_daily_summary_mv_2 TO test.wikistat_daily_summary( project LowCardinality(String), date Date, min_hits_per_hour AggregateFunction(min, UInt64), max_hits_per_hour AggregateFunction(max, UInt64), avg_hits_per_hour AggregateFunction(avg, UInt64)) AS SELECT project, toDate(time) AS date, minState(hits) AS min_hits_per_hour, maxState(hits) AS max_hits_per_hour, avgState(hits) AS avg_hits_per_hour FROM test.wikistat GROUP BY project, date;"
${CNCH_WRITE_WORKER_CLIENT} -n --query="set prefer_cnch_catalog=1; insert into table test.wikistat values('2023-06-15', 'project_1', 'subproject_2', 'path', 23), ('2023-06-15', 'project_1', 'subproject_2', 'path', 84);"
${CLICKHOUSE_CLIENT} --query="SELECT project, toDate(time) AS date, min(hits) , max(hits) , avg(hits) FROM test.wikistat GROUP BY project, date settings enable_optimizer = 1;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.wikistat_daily_summary_mv;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.wikistat_daily_summary_mv_2;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.wikistat_daily_summary;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.wikistat;"
