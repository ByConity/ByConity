drop table if exists sample_d;

CREATE TABLE sample_d (d Date DEFAULT '2000-01-01', x UInt8) ENGINE = CnchMergeTree order by (d,x) sample by x;
INSERT INTO sample_d (x) SELECT toUInt8(number) AS x FROM system.numbers LIMIT 1000100;

SELECT count() < 1000000 from ( SELECT * FROM sample_d SAMPLE 500000 ) SETTINGS enable_sample_by_range=1;

drop table if exists sample_d;
