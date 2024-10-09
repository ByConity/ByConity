USE test;
DROP TABLE IF EXISTS sp2;
SET storage_policy = 'cnch_default_hdfs1';
CREATE TABLE sp2 (a UInt32) ENGINE = CnchMergeTree() ORDER BY a; -- { serverError 478 } --


