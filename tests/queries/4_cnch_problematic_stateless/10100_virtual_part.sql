DROP TABLE IF EXISTS test.vp;
CREATE TABLE test.vp(id UInt64) ENGINE = CnchMergeTree() ORDER BY id SETTINGS index_granularity=2;
INSERT INTO test.vp SELECT * FROM system.numbers limit 100;
SELECT count() FROM test.vp SETTINGS enable_virtual_part=1;
DROP TABLE IF EXISTS test.vp;
