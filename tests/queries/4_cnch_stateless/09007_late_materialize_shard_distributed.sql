DROP TABLE IF EXISTS mergetree_00588;
DROP TABLE IF EXISTS distributed_00588;

CREATE TABLE mergetree_00588 (x UInt64, s String) ENGINE = CnchMergeTree ORDER BY x SETTINGS enable_late_materialize = 1;
INSERT INTO mergetree_00588 VALUES (1, 'hello'), (2, 'world');

SELECT * FROM mergetree_00588 WHERE x = 1 AND s LIKE '%l%' ORDER BY x, s;
-- SELECT * FROM remote('127.0.0.{1,2,3}', currentDatabase(0), mergetree_00588) WHERE x = 1 AND s LIKE '%l%' ORDER BY x, s;

SELECT * FROM mergetree_00588 PREWHERE x = 1 WHERE s LIKE '%l%' ORDER BY x, s;

DROP TABLE mergetree_00588;
