SET enable_optimizer=1;

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS t40053;

CREATE TABLE t40053 (`tea_app_id` UInt32, `event` LowCardinality(String), `event_date` Date, `hash_uid` UInt64, `event_priority` UInt16, `user_unique_id` String, `string_params` Map(String, LowCardinality(Nullable(String))))
ENGINE = CnchMergeTree
PARTITION BY (tea_app_id, event_date, event_priority)
ORDER BY (tea_app_id, event, event_date, hash_uid, user_unique_id)
SAMPLE BY hash_uid;


EXPLAIN
SELECT count()
FROM t40053 AS ds_2
PREWHERE string_params{'enter_from'} IN ('homepage_fresh');

EXPLAIN
SELECT x, y
FROM
(
    SELECT count() AS x
    FROM t40053 AS ds_1
    PREWHERE string_params{'enter_from'} IN ('homepage_fresh')
) r1,
(
    SELECT count() AS y
    FROM t40053 AS ds_2
    PREWHERE string_params{'enter_to'} IN ('homepage_click')
) r2;

-- using subcolumns explicitly are not supported
EXPLAIN
SELECT count()
FROM t40053 AS ds_2
PREWHERE `__string_params__'enter_from'` IN ('homepage_fresh'); -- { serverError 47}

insert into t40053 (`tea_app_id` , `event` , `event_date`, `hash_uid` , `event_priority` , `string_params`)
values (1001, 'foo', '2023-08-01', 1, 1, {'enter_from': 'homepage_fresh'});
insert into t40053 (`tea_app_id` , `event` , `event_date`, `hash_uid` , `event_priority` , `string_params`)
values (1001, 'foo', '2023-08-01', 1, 1, {'enter_from': 'xxx'});
insert into t40053 (`tea_app_id` , `event` , `event_date`, `hash_uid` , `event_priority` , `string_params`)
values (1001, 'foo', '2023-08-01', 1, 1, {'enter_to': 'homepage_fresh'});

SELECT count()
FROM t40053 AS ds_2
PREWHERE string_params{'enter_from'} IN ('homepage_fresh');

DROP TABLE IF EXISTS t40053;
