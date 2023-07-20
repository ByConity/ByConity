CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS test.t1;
DROP TABLE IF EXISTS test.t1_local;
DROP TABLE IF EXISTS test.t2;
DROP TABLE IF EXISTS test.t2_local;
DROP TABLE IF EXISTS test.t3;
DROP TABLE IF EXISTS test.t3_local;

CREATE TABLE test.t1_local (a Int32, b Int32) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE test.t1 AS test.t1_local ENGINE = Distributed(test_shard_localhost, test, t1_local);

CREATE TABLE test.t2_local (a Int32, b Int32) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE test.t2 AS test.t2_local ENGINE = Distributed(test_shard_localhost, test, t2_local);

CREATE TABLE test.t3_local (a Int32, b Int32) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE test.t3 AS test.t3_local ENGINE = Distributed(test_shard_localhost, test, t3_local);

use test;

INSERT INTO t1 VALUES (1, 1), (3, 1), (2, 2), (5, 1);
INSERT INTO t2 VALUES (1, 1), (2, 1), (3, 2), (6, 1);
INSERT INTO t3 VALUES (1, 1), (1, 1), (1, 2), (8, 1);

select a from t1 where (a>1 and (a in (select a as a2 from t2))) or (a<10 and (a in (select a as a3 from t3))) order by a SETTINGS join_use_nulls=0, enable_optimizer = 1;

select a from t1 where (a>1 and (a in (select a as a2 from t2))) or (a<10 and (a in (select a as a3 from t3))) order by a SETTINGS join_use_nulls=1, enable_optimizer = 1;

DROP TABLE IF EXISTS test.t1;
DROP TABLE IF EXISTS test.t1_local;
DROP TABLE IF EXISTS test.t2;
DROP TABLE IF EXISTS test.t2_local;
DROP TABLE IF EXISTS test.t3;
DROP TABLE IF EXISTS test.t3_local;
