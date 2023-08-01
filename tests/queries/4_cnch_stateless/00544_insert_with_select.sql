DROP TABLE IF EXISTS test;

CREATE TABLE test(number UInt64, num2 UInt64) ENGINE = CnchMergeTree() ORDER BY number;

INSERT INTO test WITH number * 2 AS num2 SELECT number, num2 FROM system.numbers LIMIT 3;

SELECT * FROM test;

INSERT INTO test SELECT * FROM test;

SELECT * FROM test;

CREATE TABLE source( `d` Date, `i` Int32) ENGINE = CnchMergeTree PARTITION BY d ORDER BY i;
INSERT INTO source values ('2020-01-01', 1);
CREATE TABLE target( `d` Date, `i` Int32) ENGINE = CnchMergeTree PARTITION BY d ORDER BY i;
INSERT INTO target SELECT * FROM source SETTINGS virtual_warehouse='vw_default', virtual_warehouse_write='vw_default';
SELECt * from target;

DROP TABLE source;
DROP TABLE target;
DROP TABLE test;
