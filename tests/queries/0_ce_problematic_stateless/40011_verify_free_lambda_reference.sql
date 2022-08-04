DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_local;

CREATE TABLE t_local (a Int, arr Array(Int32)) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE t AS t_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_local);

INSERT INTO t_local VALUES (10, [1, 2, 3]);

SELECT any(arrayMap(x -> x + 1, arr)) FROM t;
SELECT arrayMap(x -> x + sum(a), arr) FROM t GROUP BY arr;
SELECT arrayMap(x -> x + sum(a), any(arrayMap(x -> x + 1, arr))) FROM t;
SELECT arrayMap(x -> x + sum(x + 1), arr) FROM t GROUP BY arr; -- { serverError 47 }

-- TODO: query fails when optimizer is off
-- SELECT arrayMap(x -> x + sum(1) OVER (ORDER BY a), arr) FROM t;

SELECT arrayMap(x -> x + sum(x) OVER (ORDER BY a), arr) FROM t; -- { serverError 47 }

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_local;
