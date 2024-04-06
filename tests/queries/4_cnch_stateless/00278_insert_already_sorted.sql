create database if not exists test_00278 engine = Cnch;
use test_00278;
SET enable_shuffle_with_order=1;
SET enable_distinct_to_aggregate=0;
SET max_threads = 1;

DROP TABLE IF EXISTS sorted;
CREATE TABLE sorted (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY x SETTINGS index_granularity = 8192;

INSERT INTO sorted (x) SELECT intDiv(number, 100000) AS x FROM system.numbers LIMIT 1000000 settings max_insert_threads=1;

SELECT count() FROM sorted;
SELECT DISTINCT x FROM sorted;

INSERT INTO sorted (x) SELECT (intHash64(number) % 1000 = 0 ? 999 : intDiv(number, 100000)) AS x FROM system.numbers LIMIT 1000000 settings max_insert_threads=1;

SELECT count() FROM sorted;
SELECT DISTINCT x FROM sorted;

DROP TABLE sorted;
