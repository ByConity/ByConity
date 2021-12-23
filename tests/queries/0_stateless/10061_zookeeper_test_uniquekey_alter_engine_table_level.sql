set database_atomic_wait_for_drop_and_detach_synchronously = 1;
CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS test.change_engine;

CREATE TABLE test.change_engine (d Date, i Int, v Int)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test_change_engine', 'rep1')
PARTITION BY d ORDER BY i UNIQUE KEY i SETTINGS partition_level_unique_keys=0;

SELECT sleep(3) format Null;

INSERT INTO test.change_engine VALUES ('2020-02-02', 2, 2), ('2020-02-02', 2, 3);
INSERT INTO test.change_engine VALUES ('2020-02-02', 3, 2), ('2020-02-02', 3, 3);
INSERT INTO test.change_engine VALUES ('2020-02-02', 4, 2), ('2020-02-04', 4, 4);
INSERT INTO test.change_engine VALUES ('2020-02-02', 4, 2), ('2020-02-04', 4, 4);

SELECT * FROM test.change_engine ORDER BY d, i;

OPTIMIZE table test.change_engine;

-- FIXME (UNIQUE KEY): Support later
-- -- Alter a HaUniqueMergeTree to UniqueMergeTree
-- SET rm_zknodes_while_alter_engine = 1;
-- ALTER TABLE test.change_engine ENGINE = UniqueMergeTree;

-- SHOW CREATE test.change_engine;

-- -- Alter a UniqueMergeTree to HaUniqueMergeTree
-- ALTER TABLE test.change_engine ENGINE = HaUniqueMergeTree('/clickhouse/tables/test_change_engine_test_new', 'rep1');

-- INSERT INTO test.change_engine VALUES ('2020-02-07', 7, 2), ('2020-02-07', 7, 3);
-- INSERT INTO test.change_engine VALUES ('2020-02-08', 8, 2), ('2020-02-08', 8, 4);
-- INSERT INTO test.change_engine VALUES ('2020-02-09', 9, 2), ('2020-02-09', 9, 4);

-- SHOW CREATE test.change_engine;
-- SELECT * FROM test.change_engine ORDER BY d, i;

DROP TABLE IF EXISTS test.change_engine;
