DROP TABLE IF EXISTS test.t_test_merges;
CREATE TABLE test.t_test_merges(k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY k ORDER BY m;

SYSTEM START MERGES test.t_test_merges;

-- should not throw any exceptions.
TRY OPTIMIZE TABLE test.t_test_merges;

DROP TABLE test.t_test_merges;
