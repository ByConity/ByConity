CREATE DATABASE IF NOT EXISTS test1;

DROP TABLE IF EXISTS test1.t1;
DROP TABLE IF EXISTS test1.t1_local;

CREATE TABLE test1.t1_local (a Int32, b Int32) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE test1.t1 AS test1.t1_local ENGINE = Distributed(test_shard_localhost, test1, t1_local);

INSERT INTO test1.t1_local VALUES(1,2)(2,3);

SELECT t1.* FROM test1.t1;
SELECT test1.t1.* FROM test1.t1;

DROP TABLE IF EXISTS test1.t1;
DROP TABLE IF EXISTS test1.t1_local;

DROP DATABASE IF EXISTS test1;