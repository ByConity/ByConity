DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t;

CREATE TABLE t_local (a Int32, b Int32) ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_local VALUES (1, 1) (1, 2) (2, 1) (2, 2);
CREATE TABLE t AS t_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_local);

-- when optimizer is off, query output depends on table engine, i.e. query
-- 'select -a as a from t_local order by a;' have a difference output with query
-- 'select -a as a from t order by a;'
set enable_optimizer=1;

SELECT t.a AS origin_a, t.b AS origin_b, -a AS a
FROM t
ORDER BY -a ASC, -b ASC;

SELECT t.a AS origin_a, t.b AS origin_b, -a AS a
FROM t
ORDER BY -t.a ASC, -t.b ASC;

SELECT t.a AS origin_a, t.b AS origin_b, -a AS a
FROM t
ORDER BY a ASC, b ASC;

DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t;
