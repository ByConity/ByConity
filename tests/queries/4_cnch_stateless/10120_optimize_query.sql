SET mutations_sync = 1;

DROP TABLE IF EXISTS test_optimize_10120;
CREATE TABLE test_optimize_10120(x Int32, y String) Engine = CnchMergeTree ORDER BY x;

INSERT INTO test_optimize_10120 VALUES(1, '1');
INSERT INTO test_optimize_10120 VALUES(2, '2');
INSERT INTO test_optimize_10120 VALUES(3, '3');
INSERT INTO test_optimize_10120 VALUES(4, '4');
INSERT INTO test_optimize_10120 VALUES(5, '5');

SYSTEM START MERGES test_optimize_10120;
OPTIMIZE TABLE test_optimize_10120 PARTITION ID 'all';

SELECT * FROM test_optimize_10120;

SELECT count() FROM system.cnch_parts WHERE database = currentDatabase() AND table = 'test_optimize_10120' AND part_type = 'VisiblePart';

DROP TABLE test_optimize_10120;
