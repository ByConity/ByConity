DROP TABLE IF EXISTS test.alter_attach1;
CREATE TABLE test.alter_attach1 (c1 Date, c2 UInt32, c3 String) ENGINE = CnchMergeTree ORDER BY tuple() PARTITION BY (c1, c2);
INSERT INTO test.alter_attach1 VALUES ('2021-10-01', 1001, 'test1'), ('2021-10-02', 1002, 'test2'), ('2021-10-03', 1003, 'test3');

--1. Test normal case: can attach from table with same data structure;
DROP TABLE IF EXISTS test.alter_attach2;
CREATE TABLE test.alter_attach2 (c1 Date, c2 UInt32, c3 String) ENGINE = CnchMergeTree ORDER BY tuple() PARTITION BY (c1, c2);
ALTER TABLE test.alter_attach1 DETACH PARTITION ('2021-10-01', 1001);
ALTER TABLE test.alter_attach2 ATTACH DETACHED PARTITION ('2021-10-01', 1001) FROM test.alter_attach1;
SELECT * FROM test.alter_attach2;

--2. Test attach from table with different data structure. Will throw exception
DROP TABLE IF EXISTS test.alter_attach3;
CREATE TABLE test.alter_attach3 (c1 Date, c2 UInt32, c4 String) ENGINE = CnchMergeTree ORDER BY tuple() PARTITION BY (c1, c2);
ALTER TABLE test.alter_attach1 DETACH PARTITION ('2021-10-02', 1002);
ALTER TABLE test.alter_attach3 ATTACH DETACHED PARTITION ('2021-10-02', 1002) FROM test.alter_attach1 ; -- { serverError 122 }

--3. Test attach from table with same data structure but same partition key order in columns' definition. Will throw exception.
DROP TABLE IF EXISTS test.alter_attach4;
CREATE TABLE test.alter_attach4 (c2 UInt32, c1 Date, c3 String) ENGINE = CnchMergeTree ORDER BY tuple() PARTITION BY (c1, c2);
ALTER TABLE test.alter_attach1 DETACH PARTITION ('2021-10-03', 1003);
ALTER TABLE test.alter_attach4 ATTACH DETACHED PARTITION ('2021-10-03', 1003) FROM test.alter_attach1; -- { serverError 122 }

-- clean tables
DROP TABLE IF EXISTS test.alter_attach1;
DROP TABLE IF EXISTS test.alter_attach2;
DROP TABLE IF EXISTS test.alter_attach3;
DROP TABLE IF EXISTS test.alter_attach4;