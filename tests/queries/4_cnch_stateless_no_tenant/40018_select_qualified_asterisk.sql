use test;

DROP TABLE IF EXISTS test.test40018;

CREATE TABLE test.test40018 (a Int32, b Int32) ENGINE = CnchMergeTree() ORDER BY a;

INSERT INTO test.test40018 VALUES(1,2)(2,3);

SELECT test40018.* FROM test.test40018;
SELECT test.test40018.* FROM test.test40018;

DROP TABLE IF EXISTS test.test40018;

DROP DATABASE IF EXISTS test;