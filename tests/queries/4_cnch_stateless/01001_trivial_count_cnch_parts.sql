set mutations_sync = 1;
DROP TABLE IF EXISTS t_delete_rows;
DROP TABLE IF EXISTS t_delete_rows_u;

CREATE TABLE t_delete_rows(d Date, k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY d ORDER BY k;
SYSTEM START MERGES t_delete_rows;
INSERT INTO t_delete_rows SELECT '2024-01-01', number, number FROM numbers(5);
ALTER TABLE t_delete_rows DELETE WHERE m < 3;
SELECT rows, delete_rows FROM system.cnch_parts WHERE database = currentDatabase() AND table = 't_delete_rows' AND active SETTINGS enable_multiple_tables_for_cnch_parts = 1;

CREATE TABLE t_delete_rows_u(d Date, k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY d ORDER BY k UNIQUE KEY k;
SYSTEM START MERGES t_delete_rows_u;
INSERT INTO t_delete_rows_u SELECT '2024-01-01', number, number FROM numbers(5);
DELETE FROM t_delete_rows_u WHERE m < 3;
SELECT sum(rows), sum(delete_rows) FROM system.cnch_parts WHERE database = currentDatabase() AND table = 't_delete_rows' AND active SETTINGS enable_multiple_tables_for_cnch_parts = 1;

DROP TABLE t_delete_rows;
DROP TABLE t_delete_rows_u;

DROP TABLE IF EXISTS t_trivial_count_ttl;
CREATE TABLE t_trivial_count_ttl (p DateTime, k Int32) ENGINE = CnchMergeTree() PARTITION BY toDate(p) ORDER BY k TTL toDate(p) + INTERVAL 10 DAY;
INSERT INTO t_trivial_count_ttl VALUES (now() - INTERVAL 3 DAY, 1), (now() - INTERVAL 5 DAY, 1), (now(), 1);
SELECT count() FROM t_trivial_count_ttl;
ALTER TABLE t_trivial_count_ttl MODIFY TTL toDate(p) + INTERVAL 1 DAY;
SELECT count() FROM t_trivial_count_ttl;
DROP TABLE t_trivial_count_ttl;

