DROP TABLE IF EXISTS test.test;

CREATE TABLE test.test(number UInt64, num2 UInt64) ENGINE = CnchMergeTree() ORDER BY number;

INSERT INTO test.test WITH number * 2 AS num2 SELECT number, num2 FROM system.numbers LIMIT 3;

SELECT * FROM test.test;

INSERT INTO test.test SELECT * FROM test.test;

SELECT * FROM test.test;

DROP TABLE test.test;
