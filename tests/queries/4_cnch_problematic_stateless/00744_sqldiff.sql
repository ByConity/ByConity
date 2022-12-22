
DROP TABLE IF EXISTS diff_test1;
DROP TABLE IF EXISTS diff_test2;

CREATE TABLE diff_test1 ( uid UInt64,  a Int32,  b String) ENGINE = CnchMergeTree ORDER BY uid SETTINGS index_granularity = 8192;
CREATE TABLE diff_test2 ( uid UInt64,  b UInt64) ENGINE = CnchMergeTree ORDER BY uid SETTINGS index_granularity = 8192;

SQLDIFF table diff_test1: table diff_test2;
SQLDIFF table diff_test2: statement "CREATE TABLE diff_test1 ( uid UInt64,  a Int32,  b String) ENGINE = CnchMergeTree ORDER BY uid";


ALTER TABLE diff_test1 MODIFY COLUMN b UInt64, DROP COLUMN a;
DESCRIBE diff_test1;

ALTER TABLE diff_test2 MODIFY COLUMN b String, ADD COLUMN a Int32;
DESCRIBE diff_test2;

DROP TABLE diff_test1;
DROP TABLE diff_test2;




