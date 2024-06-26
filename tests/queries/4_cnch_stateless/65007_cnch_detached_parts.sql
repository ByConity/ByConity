DROP TABLE IF EXISTS lc_01;
CREATE TABLE lc_01 (date String,b LowCardinality(String)) ENGINE = CnchMergeTree PARTITION BY date ORDER BY b;
INSERT INTO lc_01 VALUES ('2024-06-18', 'test'), ('2024-06-18', 'test_01');
SELECT COUNT() FROM lc_01;
ALTER TABLE lc_01 DELETE WHERE b = 'test_01';
SELECT COUNT() FROM lc_01;
ALTER TABLE  lc_01 DETACH PARTITION  '2024-06-18';
SELECT count() FROM  system.cnch_detached_parts WHERE database = currentDatabase() and table = 'lc_01';
SELECT count() FROM  system.cnch_detached_parts WHERE database = currentDatabase() and table = 'lc_01' and part_type = 'VisiblePart';