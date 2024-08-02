DROP TABLE IF EXISTS test_attach_check_bytes;

CREATE TABLE test_attach_check_bytes(k Int, m Int) ENGINE = CnchMergeTree() ORDER BY k;

INSERT INTO test_attach_check_bytes VALUES (1, 1);
ALTER TABLE test_attach_check_bytes DETACH PARTITION ID 'all';
ALTER TABLE test_attach_check_bytes ATTACH PARTITION ID 'all';

SELECT bytes > 0 FROM system.cnch_parts WHERE database = currentDatabase() AND table = 'test_attach_check_bytes' AND active SETTINGS enable_multiple_tables_for_cnch_parts = 1;

DROP TABLE test_attach_check_bytes;
