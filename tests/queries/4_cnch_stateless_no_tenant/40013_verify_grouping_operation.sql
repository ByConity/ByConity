SET dialect_type='ANSI';
SET enable_optimizer=1;
SET data_type_default_nullable=false;

DROP TABLE IF EXISTS t;

CREATE TABLE t (a Int32, b Int32, c Array(Int32)) ENGINE = CnchMergeTree() ORDER BY a;

SELECT grouping(a) FROM t; -- { serverError 36 }
SELECT count(*), grouping(a) FROM t; -- { serverError 36 }
SELECT count(*), grouping(a) FROM t GROUP BY ROLLUP(a);
SELECT count(*), grouping(a) FROM t GROUP BY ROLLUP(b); -- { serverError 36 }
SELECT count(*), arrayFilter(a -> grouping(a) = 0, c) FROM t GROUP BY ROLLUP(a, c); -- { serverError 36 }
SELECT count(*), arrayFilter(b -> grouping(a) = 0, c) FROM t GROUP BY ROLLUP(a, c);

DROP TABLE IF EXISTS t;

SET dialect_type='MYSQL';
SET enable_optimizer=1;
SET data_type_default_nullable=false;

DROP TABLE IF EXISTS t;

CREATE TABLE t(a Int32, b Int32, c Array(Int32)) ENGINE = CnchMergeTree() ORDER BY a;

SELECT grouping(a) FROM t; -- { serverError 36 }
SELECT count(*), grouping(a) FROM t; -- { serverError 36 }
SELECT count(*), grouping(a) FROM t GROUP BY ROLLUP(a);
SELECT count(*), grouping(a) FROM t GROUP BY ROLLUP(b); -- { serverError 36 }
SELECT count(*), arrayFilter(a -> grouping(a) = 0, c) FROM t GROUP BY ROLLUP(a, c); -- { serverError 36 }
SELECT count(*), arrayFilter(b -> grouping(a) = 0, c) FROM t GROUP BY ROLLUP(a, c);

DROP TABLE IF EXISTS t;
