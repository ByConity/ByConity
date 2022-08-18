


DROP TABLE IF EXISTS offloading;
DROP TABLE IF EXISTS offloading2;

CREATE TABLE offloading (d Date, k UInt64) ENGINE=CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY k;

SET cnch_offloading_mode = 1;
SET enable_optimizer = 0;

INSERT INTO offloading VALUES ('2015-01-01', 10);
INSERT INTO offloading VALUES ('2015-01-02', 20);
INSERT INTO offloading VALUES ('2015-02-01', 30);
INSERT INTO offloading VALUES ('2015-02-02', 40);
INSERT INTO offloading VALUES ('2015-03-01', 50);

SELECT * FROM offloading ORDER BY k;

CREATE TABLE offloading2 AS offloading;

INSERT INTO offloading2 SELECT * FROM offloading;

SELECT * FROM offloading2 ORDER BY k;

DROP TABLE offloading;
DROP TABLE offloading2;
