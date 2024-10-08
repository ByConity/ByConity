set dialect_type='ANSI';

DROP TABLE IF EXISTS t;

CREATE TABLE t (a Int32, b Int32) ENGINE = CnchMergeTree() ORDER BY a;
INSERT INTO t VALUES (10, 10)(11, 12);

SELECT a, ( SELECT b FROM t t2 WHERE t1.a = t2.b) FROM t t1 ORDER BY a;

select * from t t1 where (select b from t t2 where t1.a = t2.a) = 10 ORDER BY a;

DROP TABLE IF EXISTS t;
