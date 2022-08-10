DROP TABLE IF EXISTS test.u10114_tr;

CREATE TABLE test.u10114_tr (d Date, k Int64, v Int64) ENGINE = CnchMergeTree PARTITION BY d ORDER BY k UNIQUE KEY k;

INSERT INTO test.u10114_tr SELECT number % 3, number, number FROM system.numbers LIMIT 1000;

SELECT 'before truncate:', count(1) FROM test.u10114_tr;

TRUNCATE TABLE test.u10114_tr;

SELECT 'after truncate:', count(1) FROM test.u10114_tr;

INSERT INTO test.u10114_tr SELECT 0, number, 0 FROM system.numbers LIMIT 1500;
INSERT INTO test.u10114_tr SELECT '2021-01-01', number, number FROM system.numbers LIMIT 1000;
INSERT INTO test.u10114_tr SELECT '2021-01-01', number + 500, number + 500 FROM system.numbers LIMIT 1000;

SELECT 'after insert';
SELECT d, count(1), sum(v) FROM test.u10114_tr GROUP BY d ORDER BY d;

DROP TABLE IF EXISTS test.u10114_tr;
