SET mutations_sync = 1;
SET disable_optimize_final = 0;

DROP TABLE IF EXISTS t_optimize_final;

CREATE TABLE t_optimize_final(k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY k ORDER BY m;
SYSTEM START MERGES t_optimize_final;
SYSTEM STOP MERGES t_optimize_final;

INSERT INTO t_optimize_final values (1,1);
INSERT INTO t_optimize_final values (1,1);
OPTIMIZE TABLE t_optimize_final FINAL;
SELECT count() FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = 't_optimize_final' AND partition_id = '1' AND active SETTINGS enable_multiple_tables_for_cnch_parts = 1;

INSERT INTO t_optimize_final values (2,1);
INSERT INTO t_optimize_final values (2,1);
INSERT INTO t_optimize_final values (2,1);
OPTIMIZE TABLE t_optimize_final PARTITION ID '2' FINAL;
SELECT count() FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = 't_optimize_final' AND partition_id = '2' AND active SETTINGS enable_multiple_tables_for_cnch_parts = 1;

DROP TABLE t_optimize_final;