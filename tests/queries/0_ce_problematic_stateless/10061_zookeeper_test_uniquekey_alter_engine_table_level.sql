set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

DROP TABLE IF EXISTS test.change_engine_r1;
DROP TABLE IF EXISTS test.change_engine_r2;

CREATE TABLE test.change_engine_r1 (d Date, i Int, v Int)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test_alter_engine', 'r1')
PARTITION BY d ORDER BY i UNIQUE KEY i SETTINGS partition_level_unique_keys=0;

CREATE TABLE test.change_engine_r2 (d Date, i Int, v Int)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test_alter_engine', 'r2')
PARTITION BY d ORDER BY i UNIQUE KEY i SETTINGS partition_level_unique_keys=0, replicated_can_become_leader = 0;

INSERT INTO test.change_engine_r1 VALUES ('2020-02-02', 2, 2), ('2020-02-02', 2, 3);
INSERT INTO test.change_engine_r1 VALUES ('2020-02-02', 3, 2), ('2020-02-02', 3, 3);
INSERT INTO test.change_engine_r1 VALUES ('2020-02-02', 4, 2), ('2020-02-04', 4, 4);
INSERT INTO test.change_engine_r1 VALUES ('2020-02-02', 4, 2), ('2020-02-04', 4, 4);

SELECT * FROM test.change_engine_r1 ORDER BY d, i;

OPTIMIZE table test.change_engine_r1;

-- FIXME (UNIQUE KEY): Support later
-- -- Alter a HaUniqueMergeTree to UniqueMergeTree
-- SET rm_zknodes_while_alter_engine = 1;
-- ALTER TABLE test.change_engine_r1 ENGINE = UniqueMergeTree;

-- SHOW CREATE test.change_engine_r1;

-- -- Alter a UniqueMergeTree to HaUniqueMergeTree
-- ALTER TABLE test.change_engine_r1 ENGINE = HaUniqueMergeTree('/clickhouse/tables/test_change_engine_test_new', 'r1');

-- INSERT INTO test.change_engine_r1 VALUES ('2020-02-07', 7, 2), ('2020-02-07', 7, 3);
-- INSERT INTO test.change_engine_r1 VALUES ('2020-02-08', 8, 2), ('2020-02-08', 8, 4);
-- INSERT INTO test.change_engine_r1 VALUES ('2020-02-09', 9, 2), ('2020-02-09', 9, 4);

-- SHOW CREATE test.change_engine_r1;
-- SELECT * FROM test.change_engine_r1 ORDER BY d, i;

DROP TABLE IF EXISTS test.change_engine_r1;
DROP TABLE IF EXISTS test.change_engine_r2;
