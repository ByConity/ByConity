DROP TABLE IF EXISTS 01593_alter_map_column1;
DROP TABLE IF EXISTS 01593_alter_map_column2;

CREATE TABLE 01593_alter_map_column1 (
    n UInt8,
    `string_map` Map(String, String),
    `string_array_map` Map(String, Array(String)),
    `int_map` Map(UInt32, UInt32) KV)
Engine=CnchMergeTree ORDER BY n;

CREATE TABLE 01593_alter_map_column2 (
    n UInt8,
    `string_map` Map(String, String),
    `string_array_map` Map(String, Array(String)),
    `int_map` Map(UInt32, UInt32) KV)
Engine=CnchMergeTree ORDER BY n;

SYSTEM START MERGES 01593_alter_map_column1;
SYSTEM START MERGES 01593_alter_map_column2;

INSERT INTO 01593_alter_map_column1 VALUES (1, {'s1': 's1'}, {'s1': ['v1', 'v2']}, {1: 1});
INSERT INTO 01593_alter_map_column1 VALUES (1, {'s1': 's1'}, {'s1': ['v1', 'v2']}, {1: 1});

ALTER TABLE 01593_alter_map_column1 MODIFY COLUMN `string_map` Map(String, String) KV; -- { serverError 53 }

ALTER TABLE 01593_alter_map_column1 MODIFY COLUMN `string_array_map` Map(String, String);

ALTER TABLE 01593_alter_map_column1 MODIFY COLUMN `int_map` Map(UInt32, UInt32); -- { serverError 53 }

ALTER TABLE 01593_alter_map_column1 MODIFY COLUMN `int_map` Map(String, String) KV;

ALTER TABLE 01593_alter_map_column2 RENAME COLUMN `int_map` TO `new_int_map`, RENAME COLUMN `string_map` TO `new_string_map` settings mutations_sync=1;

-- manipulations maybe unfinished even if add mutations_sync=1, sleep here is slow but can make ci stable
SELECT sleepEachRow(3) FROM numbers(10) FORMAT Null;

SELECT * FROM 01593_alter_map_column1;
SELECT * FROM 01593_alter_map_column2;


DROP TABLE 01593_alter_map_column1;
DROP TABLE 01593_alter_map_column2;
