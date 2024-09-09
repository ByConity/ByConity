SET dialect_type='ANSI';
SET enable_optimizer=1;
SET data_type_default_nullable=false;

DROP TABLE IF EXISTS t;

CREATE TABLE t (a Int32, b Int32) ENGINE = CnchMergeTree() ORDER BY a;

INSERT INTO t VALUES (10, 10);

SELECT EXISTS(SELECT 1 FROM t GROUP BY a HAVING a = t1.b) FROM t t1;
SELECT EXISTS(SELECT 1 FROM t WHERE t1.b = a) FROM t t1 GROUP BY a; -- { serverError 215 }

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_local;

SET dialect_type='MYSQL';
SET enable_optimizer=1;
SET data_type_default_nullable=false;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_local;

CREATE TABLE t (a Int32, b Int32) ENGINE = CnchMergeTree() ORDER BY a;

INSERT INTO t VALUES (10, 10);

SELECT EXISTS(SELECT 1 FROM t GROUP BY a HAVING a = t1.b) FROM t t1;
SELECT EXISTS(SELECT 1 FROM t WHERE t1.b = a) FROM t t1 GROUP BY a; -- { serverError 215 }

DROP TABLE IF EXISTS t;
