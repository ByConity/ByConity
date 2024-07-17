DROP TABLE IF EXISTS t_mutations_basic_info;
CREATE TABLE t_mutations_basic_info (k Int32, m Int32) ENGINE = CnchMergeTree() PARTITION BY k ORDER BY m;
SYSTEM START MERGES t_mutations_basic_info;
SYSTEM REMOVE MERGES t_mutations_basic_info;
INSERT INTO t_mutations_basic_info SELECT number, number FROM numbers(5);

ALTER TABLE t_mutations_basic_info DELETE WHERE m = 1;

SELECT count() FROM system.mutations WHERE database = currentDatabase(1) and table = 't_mutations_basic_info' LIMIT 1 SETTINGS system_mutations_only_basic_info = 1;

DROP TABLE t_mutations_basic_info;
