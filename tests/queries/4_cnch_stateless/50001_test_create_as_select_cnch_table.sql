DROP TABLE IF EXISTS test.source;
CREATE TABLE test.source (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    ORDER BY `id`;

INSERT INTO test.source VALUES ('2019-01-01', 1, 'a');

SHOW CREATE TABLE test.source;

DROP TABLE IF EXISTS test.target;
CREATE TABLE test.target ENGINE = CnchMergeTree()
PARTITION BY d
ORDER BY id AS SELECT * FROM test.source;
SHOW CREATE TABLE test.target;
SELECT count() FROM test.target;

DROP TABLE IF EXISTS test.source;
DROP TABLE IF EXISTS test.target;
