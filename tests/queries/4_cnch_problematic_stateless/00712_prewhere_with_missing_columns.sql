USE test;
DROP TABLE IF EXISTS test.mt;
CREATE TABLE test.mt (x UInt8, s String) ENGINE = CnchMergeTree ORDER BY tuple();

INSERT INTO test.mt VALUES (1, 'Hello, world!');
SELECT * FROM test.mt;

ALTER TABLE test.mt ADD COLUMN y UInt8 DEFAULT 0;
INSERT INTO test.mt VALUES (2, 'Goodbye.', 3);
SELECT * FROM test.mt ORDER BY x;

SELECT s FROM test.mt PREWHERE x AND y ORDER BY s;
SELECT s, y FROM test.mt PREWHERE x AND y ORDER BY s;

DROP TABLE test.mt;
