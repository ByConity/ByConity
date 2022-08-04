DROP TABLE IF EXISTS test.u10115;

CREATE TABLE test.u10115 (k Int32, v String) ENGINE = CnchMergeTree ORDER BY k UNIQUE KEY k;

INSERT INTO test.u10115 VALUES (1, '1a'), (2, '2a');

DROP TABLE test.u10115;
INSERT INTO test.u10115 VALUES (1, '1a'), (2, '2a'); -- { serverError 60 }
UNDROP TABLE test.u10115;
INSERT INTO test.u10115 VALUES (2, '2b'), (3, '3b');
SELECT 'after drop, undrop, insert';
SELECT * FROM test.u10115 ORDER BY k;

DROP TABLE test.u10115;
UNDROP TABLE test.u10115;
INSERT INTO test.u10115 VALUES (3, '3c'), (4, '4c');
SELECT 'after drop, undrop, insert';
SELECT * FROM test.u10115 ORDER BY k;

DROP TABLE test.u10115;
CREATE TABLE test.u10115 (k Int32, v String) ENGINE = CnchMergeTree ORDER BY k UNIQUE KEY k;
INSERT INTO test.u10115 VALUES (4, '4d'), (5, '5d');
SELECT 'after drop, create, insert';
SELECT * FROM test.u10115 ORDER BY k;

DROP TABLE test.u10115;
UNDROP TABLE test.u10115;
INSERT INTO test.u10115 VALUES (5, '5e'), (6, '6e');
SELECT 'after drop, undrop, insert';
SELECT * FROM test.u10115 ORDER BY k;

DROP TABLE IF EXISTS test.u10115;
