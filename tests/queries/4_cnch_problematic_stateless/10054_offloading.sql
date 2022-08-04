USE test;


DROP TABLE IF EXISTS test.offloading;
DROP TABLE IF EXISTS test.offloading2;

CREATE TABLE test.offloading (d Date, k UInt64) ENGINE=CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY k;

SET cnch_offloading_mode = 1;
SET enable_optimizer = 0;

INSERT INTO test.offloading VALUES ('2015-01-01', 10);
INSERT INTO test.offloading VALUES ('2015-01-02', 20);
INSERT INTO test.offloading VALUES ('2015-02-01', 30);
INSERT INTO test.offloading VALUES ('2015-02-02', 40);
INSERT INTO test.offloading VALUES ('2015-03-01', 50);

SELECT * FROM offloading ORDER BY k;

CREATE TABLE test.offloading2 AS test.offloading;

INSERT INTO test.offloading2 SELECT * FROM test.offloading;

SELECT * FROM test.offloading2 ORDER BY k;

DROP TABLE test.offloading;
DROP TABLE test.offloading2;
