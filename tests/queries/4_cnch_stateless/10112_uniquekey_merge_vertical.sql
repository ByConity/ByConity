DROP TABLE IF EXISTS u10112_vertical;

CREATE TABLE u10112_vertical (c1 Int32, c2 Int32, k1 UInt32, k2 String, v1 UInt32)
ENGINE = CnchMergeTree()
ORDER BY (k1, intHash64(k1))
UNIQUE KEY (k1, k2)
SAMPLE BY intHash64(k1)
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_columns_to_activate = 0, vertical_merge_algorithm_min_rows_to_activate = 0;

-- First "START MERGES" to make sure CnchMergeMutateThread is created on server
-- Then "STOP MERGES" to stop CnchMergeMutateThread's background task so that it will not conflict with `OPTIMIZE` query
SYSTEM START MERGES u10112_vertical;
SYSTEM STOP MERGES u10112_vertical;

INSERT INTO u10112_vertical VALUES (0, 100, 1, 'a', 1), (1, 101, 1, 'b', 1), (2, 102, 1, 'c', 1);
SELECT sleep(3) FORMAT Null; -- sleep for a while because auto stats process after the first insertion may affect the result
INSERT INTO u10112_vertical VALUES (3, 103, 1, 'b', 1), (4, 104, 2, 'b', 1), (5, 105, 2, 'a', 1);
INSERT INTO u10112_vertical VALUES (6, 106, 3, 'a', 1), (7, 107, 3, 'b', 1), (8, 108, 2, 'a', 1);

SELECT '-- before merge --';
SELECT * FROM u10112_vertical order by k1, k2;
SELECT '#parts:', count(1) FROM system.cnch_parts where database=currentDatabase(1) and table='u10112_vertical' and active;

optimize table u10112_vertical final SETTINGS mutations_sync = 1, disable_optimize_final = 0;;
SELECT '-- after merge --';
SELECT * FROM u10112_vertical order by k1, k2;
SELECT '#parts:', count(1) FROM system.cnch_parts where database=currentDatabase(1) and table='u10112_vertical' and active;

DROP TABLE IF EXISTS u10112_vertical;

SELECT '';
SELECT 'test empty part with LC type';
CREATE TABLE u10112_vertical (c1 Int32, c2 Int32, k1 UInt32, k2 String, v1 LowCardinality(UInt32))
ENGINE = CnchMergeTree()
ORDER BY (k1, intHash64(k1))
UNIQUE KEY (k1, k2)
SAMPLE BY intHash64(k1)
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_columns_to_activate = 0, vertical_merge_algorithm_min_rows_to_activate = 0;

-- First "START MERGES" to make sure CnchMergeMutateThread is created on server
-- Then "STOP MERGES" to stop CnchMergeMutateThread's background task so that it will not conflict with `OPTIMIZE` query
SYSTEM START MERGES u10112_vertical;
SYSTEM STOP MERGES u10112_vertical;

INSERT INTO u10112_vertical VALUES (0, 100, 1, 'a', 1);
INSERT INTO u10112_vertical VALUES (0, 100, 1, 'a', 1);
INSERT INTO u10112_vertical VALUES (0, 100, 1, 'a', 1);

SELECT '-- before merge --';
SELECT * FROM u10112_vertical order by k1, k2;
SELECT '#parts:', count(1) FROM system.cnch_parts where database=currentDatabase(1) and table='u10112_vertical' and active;

optimize table u10112_vertical final SETTINGS mutations_sync = 1, disable_optimize_final = 0;
SELECT '-- after merge --';
SELECT * FROM u10112_vertical order by k1, k2;
SELECT '#parts:', count(1) FROM system.cnch_parts where database=currentDatabase(1) and table='u10112_vertical' and active;

DROP TABLE IF EXISTS u10112_vertical;
