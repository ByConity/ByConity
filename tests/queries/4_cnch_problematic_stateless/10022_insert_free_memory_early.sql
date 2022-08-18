
DROP TABLE IF EXISTS data;
CREATE TABLE data (s String, n UInt32) ENGINE = CnchMergeTree() PARTITION BY n ORDER BY n;

SET free_resource_early_in_write = 1;
INSERT INTO data VALUES ('hello', 1);
select * FROM data;

DROP TABLE data;