DROP TABLE IF EXISTS 02681_partial_part_columns1;
DROP TABLE IF EXISTS 02681_partial_part_columns2;
DROP TABLE IF EXISTS 02681_partial_part_columns3;
DROP TABLE IF EXISTS 02681_partial_part_columns4;

CREATE TABLE 02681_partial_part_columns1 (a Int, b Date) ENGINE = CnchMergeTree ORDER BY a;
CREATE TABLE 02681_partial_part_columns2 (a Int, b Date) ENGINE = CnchMergeTree ORDER BY a;
CREATE TABLE 02681_partial_part_columns3 (a Int, b Date, c Map(String, String), d Map(String, String), e Map(String, String)) ENGINE = CnchMergeTree ORDER BY a;
CREATE TABLE 02681_partial_part_columns4 (a Int, b Nested (s1 Int, s2 Array(Int)), c Int) engine = CnchMergeTree order by a;

-- Start merge mutation threads.
SYSTEM START MERGES 02681_partial_part_columns1;
SYSTEM START MERGES 02681_partial_part_columns2;
SYSTEM START MERGES 02681_partial_part_columns3;
SYSTEM START MERGES 02681_partial_part_columns4;


SELECT 'Case1: Modify unexists columns';
INSERT into 02681_partial_part_columns1 VALUES (0, '2025-04-01');
ALTER TABLE 02681_partial_part_columns1 ADD COLUMN c Array(Nullable(String)), ADD COLUMN d Int;
-- Update columns_commit_time by drop column.
ALTER TABLE 02681_partial_part_columns1 DROP COLUMN d;
ALTER TABLE 02681_partial_part_columns1 MODIFY COLUMN IF EXISTS c Nullable(String) SETTINGS mutations_sync = 1;

-- Will have 2 invisible part and 1 visible part
select active, part_type from system.cnch_parts where database = currentDatabase() and table = '02681_partial_part_columns1' order by part_type, active;
SELECT * FROM 02681_partial_part_columns1;
DROP TABLE 02681_partial_part_columns1;


SELECT 'Case2: Attach partial part with modified columns';
INSERT INTO 02681_partial_part_columns2 VALUES (1, '2025-04-01');
ALTER TABLE 02681_partial_part_columns2 ADD COLUMN c Int64;
ALTER TABLE 02681_partial_part_columns2 DROP COLUMN c SETTINGS mutations_sync = 1; -- partial part will get empty columns
ALTER TABLE 02681_partial_part_columns2 DETACH PARTITION ID 'all';
ALTER TABLE 02681_partial_part_columns2 MODIFY COLUMN `b` String;
ALTER TABLE 02681_partial_part_columns2 ATTACH PARTITION ID 'all';

-- Will have 2 droped parts, 1 tombstone, 1 invisible part and 1 visible part
select active, part_type from system.cnch_parts where database = currentDatabase() and table = '02681_partial_part_columns2' order by part_type, active;
SELECT * FROM 02681_partial_part_columns2;
DROP TABLE 02681_partial_part_columns2;


SELECT 'Case3: Attach partial part with re-added columns, including normal columns, map, empty map';
INSERT into 02681_partial_part_columns3 VALUES (2, '2025-04-01', {'k1': 'v1'}, {}, {});
ALTER TABLE 02681_partial_part_columns3 DROP COLUMN b, DROP COLUMN c, DROP COLUMN d SETTINGS mutations_sync = 1;
ALTER TABLE 02681_partial_part_columns3 DETACH PARTITION ID 'all';
ALTER TABLE 02681_partial_part_columns3 ADD COLUMN `b` Nullable(String), ADD COLUMN c Date, ADD COLUMN d Array(String);
ALTER TABLE 02681_partial_part_columns3 ATTACH PARTITION ID 'all';

-- Will have 2 droped parts, 1 tombstone, 1 invisible part and 1 visible part
select active, part_type from system.cnch_parts where database = currentDatabase() and table = '02681_partial_part_columns3' order by part_type, active;
SELECT a, b, c, d, e FROM 02681_partial_part_columns3;
DROP TABLE 02681_partial_part_columns3;


SELECT 'Case4: Attach partial part with re-added nested columns';
INSERT INTO 02681_partial_part_columns4 VALUES (0, [1, 2, 3], [[3], [2], [1]], 1);
alter table 02681_partial_part_columns4 drop column b.s2 SETTINGS mutations_sync = 1;
ALTER TABLE 02681_partial_part_columns4 DETACH PARTITION ID 'all';
alter table 02681_partial_part_columns4 add column b.s2 Array(Nullable(Int));
ALTER TABLE 02681_partial_part_columns4 ATTACH PARTITION ID 'all';

-- Will have 2 droped parts, 1 tombstone, 1 invisible part and 1 visible part
select active, part_type from system.cnch_parts where database = currentDatabase() and table = '02681_partial_part_columns4' order by part_type, active;
SELECT * FROM 02681_partial_part_columns4;
DROP TABLE 02681_partial_part_columns4;

