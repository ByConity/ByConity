
DROP TABLE IF EXISTS preallocate;
CREATE TABLE preallocate (d Date, k UInt64) ENGINE=CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY k;

INSERT INTO preallocate VALUES ('2015-01-01', 10);

SELECT * FROM preallocate ORDER BY k;

PREALLOCATE preallocate;

SELECT * FROM preallocate ORDER BY k;

DROP TABLE preallocate;
