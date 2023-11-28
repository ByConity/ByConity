SET dialect_type='ANSI', enable_optimizer=1;

DROP TABLE IF EXISTS t40073_ansi_alias;
CREATE TABLE t40073_ansi_alias(a Int32, b Int32) ENGINE = CnchMergeTree() ORDER BY tuple();
INSERT INTO t40073_ansi_alias VALUES (0, 0) (1, 1) (2, 2);

-- { echo }

-- regular alias

EXPLAIN SELECT a + 1 AS p, count() AS cnt FROM t40073_ansi_alias GROUP BY p;

SELECT a + 1 AS p, count() AS cnt FROM t40073_ansi_alias GROUP BY p ORDER BY p;

-- alias in prewhere

EXPLAIN SELECT a + 1 AS p FROM t40073_ansi_alias PREWHERE p > 1;

SELECT a + 1 AS p FROM t40073_ansi_alias PREWHERE p > 1 ORDER BY p;

-- prefer source column

EXPLAIN SELECT a + 1 AS a FROM t40073_ansi_alias WHERE a > 1;

SELECT a + 1 AS a FROM t40073_ansi_alias WHERE a > 1 ORDER BY a;

EXPLAIN SELECT a + 1 AS a FROM t40073_ansi_alias PREWHERE a > 1;

SELECT a + 1 AS a FROM t40073_ansi_alias PREWHERE a > 1 ORDER BY a;

-- prefer source column ignoring ORDER BY

EXPLAIN SELECT -a AS a, b FROM t40073_ansi_alias ORDER BY a ASC;

SELECT -a AS a, b FROM t40073_ansi_alias ORDER BY a ASC;
