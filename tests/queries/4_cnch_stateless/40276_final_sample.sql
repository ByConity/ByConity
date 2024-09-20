DROP TABLE IF EXISTS sample_40276;
DROP TABLE IF EXISTS sample_40276_local;

CREATE TABLE sample_40276 (d Date DEFAULT '2000-01-01', x UInt8) ENGINE = CnchMergeTree order by d;
INSERT INTO sample_40276 (x) SELECT toUInt8(number) AS x FROM system.numbers LIMIT 256;

SET min_block_size = 1;
SET max_block_size = 100;

SELECT count() < 256 from ( SELECT * FROM sample_40276 SAMPLE 100 ) SETTINGS enable_final_sample = 1;
SELECT count() < 102 from ( SELECT * FROM sample_40276 SAMPLE 100 ) SETTINGS enable_final_sample = 1;
SELECT count() < 102 from ( SELECT * FROM sample_40276 SAMPLE 100 ) t1 join ( SELECT * FROM sample_40276 SAMPLE 100 ) t2 on t1.x=t2.x SETTINGS enable_final_sample = 1;

SET max_block_size = 200;

SELECT count() < 202 from ( SELECT * FROM sample_40276 SAMPLE 100 ) SETTINGS enable_final_sample = 1;
SELECT count() < 202 from ( SELECT * FROM sample_40276 SAMPLE 100 ) t1 join ( SELECT * FROM sample_40276 SAMPLE 100 ) t2 on t1.x=t2.x SETTINGS enable_final_sample = 1;

DROP TABLE IF EXISTS sample_40276;
DROP TABLE IF EXISTS sample_40276_local;
