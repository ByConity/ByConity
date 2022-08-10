DROP TABLE IF EXISTS test.test;
CREATE TABLE test.test(date Date, keys Array(Nullable(UInt8))) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(date) ORDER BY date SETTINGS index_granularity=1;
INSERT INTO test.test VALUES ('2017-09-10', [1, 2, 3, 4, 5, 6, 7, NULL]);
SELECT * FROM test.test LIMIT 1;
SELECT avgArray(keys) FROM test.test;
DROP TABLE test.test;
