SET dialect_type='ANSI';
SET data_type_default_nullable=false;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_local;

CREATE TABLE t_local (a Int32, b Int32) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE t AS t_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_local);

INSERT INTO t_local VALUES (10, 10);

SELECT EXISTS(SELECT 1 FROM t GROUP BY a HAVING a = t1.b) FROM t t1;
SELECT EXISTS(SELECT 1 FROM t WHERE t1.b = a) FROM t t1 GROUP BY a; -- { serverError 215 }

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_local;
