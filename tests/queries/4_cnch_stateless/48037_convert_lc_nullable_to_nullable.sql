set data_type_default_nullable=1;

drop table if exists 48037_table;
CREATE TABLE 48037_table
(
    `int_col_1` UInt64 NOT NULL,
    `int_col_2` Nullable(UInt64),
    `int_col_3` LowCardinality(Int8),
    `int_col_4` LowCardinality(Nullable(UInt64)),
    `int_col_5` UInt128 NOT NULL,
    `int_col_6` LowCardinality(Nullable(UInt128)),
    `str_col_1` String NOT NULL,
    `str_col_2` LowCardinality(Nullable(String)),
    `float_col_1` Float64 NOT NULL,
    `float_col_2` LowCardinality(Nullable(Float64)),
    `date_col_1` Date32 NOT NULL,
    `date_col_2` DateTime('Asia/Istanbul'),
    `enum_col_1` Enum('a' = 1, 'b' = 2, 'c' = 3, 'd' = 4),
    `map_col_1` Map(String, String),
    `map_col_2` Map(String, UInt64),
    `map_col_3` Map(String, LowCardinality(Nullable(String)))
)
ENGINE = CnchMergeTree
PARTITION BY (toDate(toStartOfDay(date_col_1)), int_col_5)
CLUSTER BY int_col_1 INTO 10 BUCKETS
PRIMARY KEY (int_col_1, str_col_1)
ORDER BY (int_col_1, str_col_1, float_col_1)
UNIQUE KEY (int_col_1, str_col_1)
SETTINGS index_granularity = 8192;
