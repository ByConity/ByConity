DROP TABLE IF EXISTS t_test_merges;
CREATE TABLE t_test_merges(k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY k ORDER BY m;

SYSTEM START MERGES t_test_merges;

-- should not throw any exceptions.
TRY OPTIMIZE TABLE t_test_merges;

DROP TABLE t_test_merges;
