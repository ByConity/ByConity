set enable_optimizer=1;

drop table if exists table_misc;
drop table if exists daily_misc;
CREATE TABLE table_misc  (`hash_uid` UInt64, `event_date` Date) ENGINE = CnchMergeTree PARTITION BY (event_date) CLUSTER BY hash_uid INTO 100 BUCKETS ORDER BY (hash_uid);
CREATE TABLE daily_misc  (`p_date` Date, `hash_uid` UInt64) ENGINE = CnchMergeTree PARTITION BY (p_date) CLUSTER BY hash_uid INTO 100 BUCKETS ORDER BY (hash_uid) SAMPLE BY hash_uid;
explain select count(*) from table_misc t, daily_misc d where t.hash_uid = d.hash_uid and event_date = p_date;
explain select count(a) from (select *, hash_uid+hash_uid a from table_misc) t, daily_misc d where t.hash_uid = d.hash_uid and event_date = p_date;
select count(*) from table_misc t, daily_misc d where t.hash_uid = d.hash_uid and event_date = p_date group by d.p_date settings enum_repartition=0;




drop TABLE if exists join_1;
CREATE TABLE join_1
(
    `int_col_1` Int64,
    `int_col_2` Nullable(Int64),
    `str_col_1` Nullable(String),
    `str_col_2` Nullable(String),
    `float_col_1` Nullable(Float64),
    `float_col_2` Nullable(Float64),
    `date_col` Date,
    `datetime_col` DateTime,
    `map_col_str` Map(String, LowCardinality(Nullable(String))),
    `array_col_int` Array(Int64)
)
ENGINE = CnchMergeTree
PARTITION BY toDate(date_col)
CLUSTER BY int_col_2 % 20 INTO 20 BUCKETS order by date_col;

EXPLAIN stats=0
SELECT
    int_col_1,
    int_col_2
FROM
(
    SELECT
        int_col_1,
        int_col_2
    FROM join_1
    WHERE (int_col_1 > 98) AND (date_col = '2024-10-20')
) AS a
INNER JOIN
(
    SELECT
        int_col_1,
        int_col_2
    FROM join_1
    WHERE (int_col_1 > 98) AND (date_col = '2024-10-20')
) AS b ON a.int_col_2 = b.int_col_1 settings enum_replicate_no_stats=0;

drop TABLE if exists join_1;
drop table if exists table_misc;
drop table if exists daily_misc;
