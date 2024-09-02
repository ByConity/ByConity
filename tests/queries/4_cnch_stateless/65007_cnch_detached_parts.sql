DROP TABLE IF EXISTS lc_01;
CREATE TABLE lc_01 (date String, b LowCardinality(String)) ENGINE = CnchMergeTree PARTITION BY date ORDER BY b;
SYSTEM START MERGES lc_01;
INSERT INTO lc_01 VALUES ('2024-06-18', 'test'), ('2024-06-18', 'test_01');
SELECT COUNT() FROM lc_01;
ALTER TABLE lc_01 DELETE WHERE b = 'test_01' SETTINGS mutations_sync = 1;
-- MergeMutateThread will wait ~3 seconds before scheduling.
SELECT sleepEachRow(3) FROM numbers(40) FORMAT Null;
SELECT COUNT() FROM lc_01;
ALTER TABLE  lc_01 DETACH PARTITION  '2024-06-18';
SELECT count() FROM  system.cnch_detached_parts WHERE database = currentDatabase(1) and table = 'lc_01';
SELECT count() FROM  system.cnch_detached_parts WHERE database = currentDatabase(1) and table = 'lc_01' and part_type = 'VisiblePart';
