
DROP TABLE IF EXISTS partitions;
CREATE TABLE partitions (x UInt64) ENGINE = CnchMergeTree ORDER BY x PARTITION BY x;
SET max_partitions_per_insert_block = 1;
INSERT INTO partitions SELECT * FROM system.numbers LIMIT 1;
INSERT INTO partitions SELECT * FROM system.numbers LIMIT 2; -- { serverError 252 }
ALTER TABLE partitions MODIFY SETTING max_partitions_per_insert_block = 3;
INSERT INTO partitions SELECT * FROM system.numbers LIMIT 3;
DROP TABLE partitions;
