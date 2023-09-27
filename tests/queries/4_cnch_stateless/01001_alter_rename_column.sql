DROP TABLE IF EXISTS test.t_alter_rename_column;
CREATE TABLE test.t_alter_rename_column(k Int32, m Int32) ENGINE = CnchMergeTree ORDER BY k;
SYSTEM START MERGES test.t_alter_rename_column;

INSERT INTO test.t_alter_rename_column VALUES(1,1);
ALTER TABLE test.t_alter_rename_column RENAME COLUMN m to m1;

SELECT m1 FROM test.t_alter_rename_column SETTINGS enable_optimizer = 0;
SELECT m1 FROM test.t_alter_rename_column SETTINGS enable_optimizer = 1;

SELECT sleepEachRow(3) FROM numbers(2) FORMAT Null;

SELECT m1 FROM test.t_alter_rename_column SETTINGS enable_optimizer = 0;
SELECT m1 FROM test.t_alter_rename_column SETTINGS enable_optimizer = 1;

DROP TABLE test.t_alter_rename_column;
