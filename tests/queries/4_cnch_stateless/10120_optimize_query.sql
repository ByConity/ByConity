SET mutations_sync = 1;

DROP TABLE IF EXISTS test_optimize;
CREATE TABLE test_optimize(x Int32, y String) Engine = CnchMergeTree ORDER BY x;

INSERT INTO test_optimize VALUES(1, '1');
INSERT INTO test_optimize VALUES(2, '2');
INSERT INTO test_optimize VALUES(3, '3');
INSERT INTO test_optimize VALUES(4, '4');
INSERT INTO test_optimize VALUES(5, '5');

SYSTEM START MERGES test_optimize;
OPTIMIZE TABLE test_optimize PARTITION ID 'all';

SELECT * FROM test_optimize order by x;

SELECT count() FROM system.cnch_parts WHERE database = currentDatabase() AND table = 'test_optimize' AND part_type = 'VisiblePart';

DROP TABLE test_optimize;
