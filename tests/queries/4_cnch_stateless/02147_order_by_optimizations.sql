SET enable_optimizer=0;

CREATE TABLE clickhouse_types_test
(
    `id` UInt64,
    `int8_t` Int8,
    `uint8_t` UInt8,
    `int16_t` Int16,
    `int32_t` Int32,
    `int64_t` Int64,
    `uint64_t` UInt64,
    `float32_t` Float32,
    `float64_t` Float64,
    `date_t` Date,
    `varchar_t` String,
    `datetime_str1` String,
    `datetime_str2` String,
    `date` Date
)
ENGINE = CnchMergeTree
PARTITION BY date
ORDER BY (id, date, intHash64(id))
SAMPLE BY intHash64(id);

SELECT * FROM `clickhouse_types_test` clickhouse_types_test WHERE `clickhouse_types_test`.`datetime_str1` > '1970-01-01 00:00:00' ORDER BY toDateOrNull(`clickhouse_types_test`.`datetime_str1`)
