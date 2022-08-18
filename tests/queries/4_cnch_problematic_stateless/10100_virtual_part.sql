DROP TABLE IF EXISTS vp;
CREATE TABLE vp(id UInt64) ENGINE = CnchMergeTree() ORDER BY id SETTINGS index_granularity=2;
INSERT INTO vp SELECT * FROM system.numbers limit 100;
SELECT count() FROM vp SETTINGS enable_virtual_part=1;
DROP TABLE IF EXISTS vp;
