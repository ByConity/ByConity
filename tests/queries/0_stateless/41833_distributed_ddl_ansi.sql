SET dialect_type='ANSI';
set enable_optimizer_white_list=0;

DROP TABLE IF EXISTS code_md;
CREATE TABLE code_md ON CLUSTER test_shard_localhost (
    `TYPE` String,
    `CODE`  Int32,
    `VALUE` NULLABLE(Int32)
) ENGINE = MergeTree() PRIMARY KEY CODE ORDER BY CODE;

DROP TABLE IF EXISTS code_md_dis;
CREATE TABLE code_md_dis (
     `TYPE` String,
     `CODE`  Int32,
     `VALUE` NULLABLE(Int32)
) ENGINE = Distributed('test_shard_localhost', currentDatabase(),'code_md', rand());

SET dialect_type='CLICKHOUSE';

SELECT '=====describe local table=====';
DESCRIBE TABLE code_md;
SELECT '=====describe distributed table=====';
DESCRIBE TABLE code_md_dis;

INSERT INTO code_md VALUES('TYPE_A', 10, 100), ('TYPE_B', 20, NULL), ('TYPE_C', NULL, 300);
INSERT INTO code_md_dis VALUES('TYPE_A', 40, 500), ('TYPE_B', 50, 700), ('TYPE_C', 60, NULL);
SYSTEM FLUSH DISTRIBUTED code_md_dis;

SELECT '=====query local table=====';
SELECT * FROM code_md ORDER BY CODE, TYPE, VALUE;
SELECT '=====query distributed table=====';
SELECT * FROM code_md_dis ORDER BY CODE, TYPE, VALUE;
