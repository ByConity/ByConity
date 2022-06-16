SET dialect_type='ANSI';
SET data_type_default_nullable=false;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_local;

CREATE TABLE t_local (a Int32, b Int32, c Array(Int32)) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE t AS t_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_local);

SELECT grouping(a) FROM t; -- { serverError 184 }
SELECT count(*), grouping(a) FROM t; -- { serverError 184 }
SELECT count(*), grouping(a) FROM t GROUP BY ROLLUP(a);
SELECT count(*), grouping(a) FROM t GROUP BY ROLLUP(b); -- { serverError 184 }
-- TODO: need ScopeAware
-- SELECT count(*), arrayFilter(a -> grouping(a) ? 0 : 1, c) FROM t GROUP BY ROLLUP(a, c); -- expect serverError 184 }
SELECT count(*), arrayFilter(b -> grouping(a) ? 0 : 1, c) FROM t GROUP BY ROLLUP(a, c);

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_local;
