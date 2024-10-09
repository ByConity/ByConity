set send_logs_level='none';
set optimize_trivial_count_query=0;

DROP TABLE IF EXISTS mv1;
DROP TABLE IF EXISTS `.inner.mv1`;
DROP TABLE IF EXISTS store1_1;

CREATE TABLE store1_1 (`int_col_0` Int64, `int_col_1` UInt64, `int_col_2` UInt64, `int_col_3` UInt64, `int_col_4` UInt64, `int_col_5` UInt64, `int_col_6` UInt64, `int_col_7` UInt64, `int_col_8` UInt64, `int_col_9` UInt64, `int_col_10` UInt64, `int_col_11` UInt64, `int_col_12` UInt64, `int_col_13` UInt64, `int_col_14` UInt64, `int_col_15` UInt64, `int_col_16` UInt64, `int_col_17` UInt64, `int_col_18` UInt64, `int_col_19` UInt64, `int_col_20` UInt64, `int_col_21` UInt64, `int_col_22` UInt64, `int_col_23` UInt64, `int_col_24` UInt64, `int_col_25` UInt64, `int_col_26` UInt64, `int_col_27` UInt64, `int_col_28` UInt64, `int_col_29` UInt64, `int_col_30` UInt64, `int_col_31` UInt64, `int_col_32` UInt64, `int_col_33` UInt64, `int_col_34` UInt64, `int_col_35` UInt64, `int_col_36` UInt64, `int_col_37` UInt64, `int_col_38` UInt64, `int_col_39` UInt64, `int_col_40` UInt64, `int_col_41` UInt64, `int_col_42` UInt64, `int_col_43` UInt64, `int_col_44` UInt64, `int_col_45` UInt64, `int_col_46` UInt64, `int_col_47` UInt64, `int_col_48` UInt64, `int_col_49` UInt64, `str_col_0` String, `str_col_1` String, `str_col_2` String, `str_col_3` String, `str_col_4` String, `str_col_5` String, `str_col_6` String, `str_col_7` String, `str_col_8` String, `str_col_9` String, `str_col_10` String, `str_col_11` Nullable(String), `str_col_12` Nullable(String), `str_col_13` Nullable(String), `str_col_14` Nullable(String), `str_col_15` Nullable(String), `str_col_16` Nullable(String), `str_col_17` Nullable(String), `str_col_18` Nullable(String), `str_col_19` Nullable(String), `str_col_20` Nullable(String), `str_col_21` Nullable(String), `str_col_22` Nullable(String), `str_col_23` Nullable(String), `str_col_24` Nullable(String), `str_col_25` Nullable(String), `str_col_26` Nullable(String), `str_col_27` Nullable(String), `str_col_28` Nullable(String), `str_col_29` Nullable(String), `str_col_30` Nullable(String), `str_col_31` Nullable(String), `str_col_32` Nullable(String), `str_col_33` Nullable(String), `str_col_34` Nullable(String), `str_col_35` Nullable(String), `str_col_36` Nullable(String), `str_col_37` Nullable(String), `str_col_38` Nullable(String), `str_col_39` Nullable(String), `str_col_40` Nullable(String), `str_col_41` Nullable(String), `str_col_42` Nullable(String), `str_col_43` Nullable(String), `str_col_44` Nullable(String), `str_col_45` Nullable(String), `str_col_46` Nullable(String), `str_col_47` Nullable(String), `str_col_48` Nullable(String), `str_col_49` Nullable(String), `float_col_0` Float64, `float_col_1` Float64, `float_col_2` Float64, `float_col_3` Nullable(Float64), `float_col_4` Nullable(Float64), `float_col_5` Nullable(Float64), `float_col_6` Nullable(Float64), `float_col_7` Nullable(Float64), `float_col_8` Nullable(Float64), `float_col_9` Nullable(Float64), `date_col` Date, `datetime_col` DateTime, `map_col` Map(String, String), `bitmap_col` BitMap64, `slice_id` UInt64, `array_col` Array(UInt64), `lc_col` String, `mc_col` Nullable(String), `hc_col` Nullable(String), `ipv4_col` Nullable(String), `ipv6_col` Nullable(String), `_offset` UInt64, `_partition` UInt64) ENGINE = CnchMergeTree PARTITION BY date_col ORDER BY int_col_0 SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW mv1 (`int_col_0` Int64, `sum(int_col_1)` UInt64, `date_col` Date)
ENGINE = CnchMergeTree PARTITION BY date_col ORDER BY int_col_0
AS SELECT int_col_0, sum(int_col_1), date_col FROM store1_1  GROUP BY int_col_0, date_col;

SET enable_optimizer=1, enable_materialized_view_rewrite=1;
EXPLAIN SELECT count() FROM store1_1;
EXPLAIN SELECT count(1) FROM store1_1;
EXPLAIN SELECT count(int_col_1) FROM store1_1;

DROP TABLE IF EXISTS mv1;
DROP TABLE IF EXISTS `.inner.mv1`;
DROP TABLE IF EXISTS store1_1;
