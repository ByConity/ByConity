DROP TABLE IF EXISTS test.01593_alter_map_column;

CREATE TABLE test.01593_alter_map_column (
    n UInt8,
    `string_map` Map(String, String),
    `int_map` Map(UInt32, UInt32) KV)
Engine=CnchMergeTree ORDER BY n;

INSERT INTO test.01593_alter_map_column VALUES (1, {'s1': 's1'}, {1: 1});

ALTER TABLE test.01593_alter_map_column MODIFY COLUMN `string_map` Map(String, String) KV; -- { serverError 53 }

ALTER TABLE test.01593_alter_map_column MODIFY COLUMN `int_map` Map(UInt32, UInt32); -- { serverError 53 }

SELECT * FROM test.01593_alter_map_column;

DROP TABLE test.01593_alter_map_column;
