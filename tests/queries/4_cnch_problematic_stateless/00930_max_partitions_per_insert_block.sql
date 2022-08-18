
DROP TABLE IF EXISTS partitions;
CREATE TABLE partitions (x UInt64) ENGINE = CnchMergeTree ORDER BY x PARTITION BY x;

INSERT INTO partitions SELECT * FROM system.numbers LIMIT 100;
SELECT sleep(3) FORMAT Null;
SELECT count() FROM system.cnch_parts WHERE database = 'test' AND table = 'partitions';
SYSTEM STOP MERGES partitions;
INSERT INTO partitions SELECT * FROM system.numbers LIMIT 100;
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;
SELECT count() FROM system.cnch_parts WHERE database = 'test' AND table = 'partitions';

SET max_partitions_per_insert_block = 1;

INSERT INTO partitions SELECT * FROM system.numbers LIMIT 1;
INSERT INTO partitions SELECT * FROM system.numbers LIMIT 2; -- { serverError 252 }

DROP TABLE partitions;
