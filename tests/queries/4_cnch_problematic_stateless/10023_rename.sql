USE test;
DROP TABLE IF EXISTS test.rename1;
DROP TABLE IF EXISTS test.rename2;
DROP TABLE IF EXISTS test.rename11;
DROP TABLE IF EXISTS test.rename22;

CREATE TABLE test.rename1 (d Date, a String, b String) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY d SETTINGS index_granularity = 8192;
CREATE TABLE test.rename2 (d Date, a String, b String) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY d SETTINGS index_granularity = 8192;
INSERT INTO test.rename1 VALUES ('2015-01-01', 'hello', 'world');
INSERT INTO test.rename2 VALUES ('2015-01-02', 'hello2', 'world2');
SELECT * FROM test.rename1;
SELECT * FROM test.rename2;
RENAME TABLE test.rename1 TO test.rename11, test.rename2 TO test.rename22;
SELECT * FROM test.rename11;
SELECT * FROM test.rename22;


DROP TABLE IF EXISTS test.rename1;
DROP TABLE IF EXISTS test.rename2;
DROP TABLE IF EXISTS test.rename11;
DROP TABLE IF EXISTS test.rename22;
