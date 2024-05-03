DROP TABLE IF EXISTS t_adaptive_detach;
CREATE TABLE t_adaptive_detach(k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY k ORDER BY k SETTINGS index_granularity_bytes = 1000;
INSERT INTO t_adaptive_detach VALUES(1,1);
ALTER TABLE t_adaptive_detach DETACH PARTITION '1';
DROP TABLE t_adaptive_detach;


--- Make OPTIMIZE a sync query.
SET mutations_sync = 1;

DROP TABLE IF EXISTS t_adaptive_merge;
CREATE TABLE t_adaptive_merge(k Int32, m Int32) ENGINE = CnchMergeTree ORDER BY k SETTINGS index_granularity_bytes = 1000;
SYSTEM START MERGES t_adaptive_merge;

INSERT INTO t_adaptive_merge SELECT number, number FROM numbers(3);
INSERT INTO t_adaptive_merge SELECT number, number FROM numbers(3);
INSERT INTO t_adaptive_merge SELECT number, number FROM numbers(3);
INSERT INTO t_adaptive_merge SELECT number, number FROM numbers(3);
INSERT INTO t_adaptive_merge SELECT number, number FROM numbers(3);
INSERT INTO t_adaptive_merge SELECT number, number FROM numbers(3);

OPTIMIZE TABLE t_adaptive_merge;

SELECT count() FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = 't_adaptive_merge' AND active SETTINGS enable_multiple_tables_for_cnch_parts = 1;
DROP TABLE t_adaptive_merge;