USE test;
DROP TABLE IF EXISTS test.preallocate;
CREATE TABLE test.preallocate (d Date, k UInt64) ENGINE=CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY k;

INSERT INTO test.preallocate VALUES ('2015-01-01', 10);

SELECT * FROM test.preallocate ORDER BY k;

PREALLOCATE test.preallocate;

SELECT * FROM test.preallocate ORDER BY k;

DROP TABLE test.preallocate;
