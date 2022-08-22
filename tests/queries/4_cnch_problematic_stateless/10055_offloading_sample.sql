
DROP TABLE IF EXISTS sample;

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SET max_block_size = 10;
SET cnch_offloading_mode = 1;
SET enable_optimizer = 0;

CREATE TABLE sample (d Date DEFAULT '2000-01-01', x UInt8) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) SAMPLE BY x ORDER BY x SETTINGS index_granularity = 10;

SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM sample SAMPLE 2;

INSERT INTO sample (x) SELECT toUInt8(number) AS x FROM system.numbers LIMIT 256;

SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM sample SAMPLE 2;

DROP TABLE sample;
