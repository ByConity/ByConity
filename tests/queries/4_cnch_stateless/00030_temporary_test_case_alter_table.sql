CREATE DATABASE IF NOT EXISTS test ENGINE=Cnch;
USE test;
DROP TABLE IF EXISTS alter_test
CREATE TABLE alter_test (CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32,  
NestedColumn Nested(A UInt8, S String), ToDrop UInt32)  ENGINE = CnchMergeTree 
ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID) PARTITION BY toYYYYMM(StartDate) 
SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
ALTER TABLE alter_test ADD COLUMN Added0 UInt32;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32;
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 AFTER Added0
ALTER TABLE alter_test ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;
ALTER TABLE alter_test ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;
ALTER TABLE alter_test ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1
DESC TABLE alter_test
ALTER TABLE alter_test DROP COLUMN ToDrop
ALTER TABLE alter_test MODIFY COLUMN Added0 String
ALTER TABLE alter_test DROP COLUMN NestedColumn.A;
ALTER TABLE alter_test DROP COLUMN NestedColumn.S
ALTER TABLE alter_test DROP COLUMN AddedNested1.B
ALTER TABLE alter_test ADD COLUMN IF NOT EXISTS Added0 UInt32;
ALTER TABLE alter_test ADD COLUMN IF NOT EXISTS AddedNested1 Nested(A UInt32, B UInt64);
ALTER TABLE alter_test ADD COLUMN IF NOT EXISTS AddedNested1.C Array(String);
ALTER TABLE alter_test MODIFY COLUMN IF EXISTS ToDrop UInt64;
ALTER TABLE alter_test DROP COLUMN IF EXISTS ToDrop;
ALTER TABLE alter_test COMMENT COLUMN IF EXISTS ToDrop 'new comment'
DESC TABLE alter_test
DROP TABLE alter_test
DROP TABLE IF EXISTS test.memory_buffer_test;
CREATE TABLE test.memory_buffer_test (i Int64, ts DateTime)
    ENGINE = CnchMergeTree PARTITION BY toDate(ts) ORDER BY ts SETTINGS index_granularity = 8192, cnch_enable_memory_buffer = 0
SHOW CREATE TABLE test.memory_buffer_test;
ALTER TABLE test.memory_buffer_test MODIFY SETTING cnch_enable_memory_buffer = 1;
SHOW CREATE TABLE test.memory_buffer_test;
DROP TABLE test.memory_buffer_test;
