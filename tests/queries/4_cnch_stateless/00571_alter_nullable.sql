

DROP TABLE IF EXISTS nullable;
CREATE TABLE nullable (x String) ENGINE = CnchMergeTree ORDER BY x;
INSERT INTO nullable VALUES ('hello'), ('world');
SELECT * FROM nullable;
ALTER TABLE nullable ADD COLUMN n Nullable(UInt64);
SELECT * FROM nullable;
ALTER TABLE nullable DROP COLUMN n;
ALTER TABLE nullable ADD COLUMN n Nullable(UInt64) DEFAULT NULL;
SELECT * FROM nullable;
ALTER TABLE nullable DROP COLUMN n;
ALTER TABLE nullable ADD COLUMN n Nullable(UInt64) DEFAULT 0;
SELECT * FROM nullable;
DROP TABLE nullable;
