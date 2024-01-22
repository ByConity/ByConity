DROP TABLE IF EXISTS 00745_merge_tree_check_column_vadility;

--- test invalid column name who is map implicit key
SELECT 'test invalid column name who is map implicit key';
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `__a` String) Engine=CnchMergeTree ORDER BY n; -- { serverError 36 }
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `a` Map(String, String) KV, `a.key` String) Engine=CnchMergeTree ORDER BY n; -- { serverError 36 }
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `a` Map(String, String) KV, `a.value` String) Engine=CnchMergeTree ORDER BY n; -- { serverError 36 }
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `a` Map(String, String) KV, `a.keys` String) Engine=CnchMergeTree ORDER BY n; -- { serverError 36 }
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `a` Map(String, String) KV, `a.values` String) Engine=CnchMergeTree ORDER BY n; -- { serverError 36 }

--- test invalid column name whose type is map
SELECT '';
SELECT 'test invalid column name whose type is map';
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `a__a` Map(String, String)) Engine=CnchMergeTree ORDER BY n; -- { serverError 36 }
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `a_a_` Map(String, String)) Engine=CnchMergeTree ORDER BY n; -- { serverError 36 }

--- test invalid map key or value type
SELECT '';
SELECT 'test invalid map key or value type';
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, a Map(String, Tuple(String, String))) Engine=CnchMergeTree ORDER BY n; -- { serverError 36 }
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, a Map(Nullable(String), String)) Engine=CnchMergeTree ORDER BY n; -- { serverError 36 }
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, a Map(String, LowCardinality(String))) Engine=CnchMergeTree ORDER BY n; -- { serverError 36 }
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, a Map(String, LowCardinality(String)) KV, b Map(String, Tuple(String, String)) KV) Engine=CnchMergeTree ORDER BY n;
DROP TABLE 00745_merge_tree_check_column_vadility;

--- test create column whose name is same with func column name
SELECT '';
SELECT 'test create column whose name is same with func column name';
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `_delete_flag_` Map(String, String)) Engine=CnchMergeTree ORDER BY n UNIQUE KEY n; -- { serverError 36 }

--- test create compact map table
SELECT '';
SELECT 'test create CnchMergeTree table with enable_compact_map_data enabled';
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `a_a` Map(String, String), `_a_a` Map(String, String)) Engine=CnchMergeTree ORDER BY n SETTINGS enable_compact_map_data=1; -- { serverError 344 }


--- right column name
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `a_a` Map(String, String), `_a_a` Map(String, String)) Engine=CnchMergeTree ORDER BY n;

--- test alter compact map
SELECT '';
SELECT 'test modify enable_compact_map_data to 1';
ALTER TABLE 00745_merge_tree_check_column_vadility MODIFY SETTING enable_compact_map_data = 1; -- { serverError 344 }"

DROP TABLE 00745_merge_tree_check_column_vadility;


--- byte map with nullable value
SELECT '';
SELECT 'test create CnchMergeTree table with nullable map value';
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `a` Map(String, Nullable(String))) Engine=CnchMergeTree ORDER BY n; -- { serverError 36 }


--- test alter byte map with nullable value
SELECT '';
SELECT 'test modify map column to nullable value';
CREATE TABLE 00745_merge_tree_check_column_vadility (n UInt8, `a` Map(String, String)) Engine=CnchMergeTree ORDER BY n;
ALTER TABLE 00745_merge_tree_check_column_vadility MODIFY COLUMN `a` Map(String, Nullable(String)) ; -- { serverError 36 }"

DROP TABLE 00745_merge_tree_check_column_vadility;
