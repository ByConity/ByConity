DROP TABLE IF EXISTS 00745_merge_tree_map_data_type;

--- test KV map
SELECT 'test KV map';
CREATE TABLE 00745_merge_tree_map_data_type (
    n UInt8,
    `string_map` Map(String, String) KV,
    `string_array_map` Map(String, Array(String)) KV,
    `fixed_string_map` Map(FixedString(2), FixedString(2)) KV,
    `int_map` Map(UInt32, UInt32) KV,
    `lowcardinality_map` Map(LowCardinality(String), LowCardinality(String)) KV,
    `float_map` Map(Float32, Float32) KV,
    `date_map` Map(Date, Date) KV,
    `datetime_map` Map(DateTime('Asia/Shanghai'), DateTime('Asia/Shanghai')) KV,
    `uuid_map` Map(UUID, UUID) KV,
    `enums_map` Map(Enum8('hello' = 0, 'world' = 1, 'foo' = -1), Enum8('hello' = 0, 'world' = 1, 'foo' = -1)) KV,
    `new_enums_map` Map(String, Enum8('non-default' = 1)) KV)
Engine=CnchMergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0;

insert into 00745_merge_tree_map_data_type values(1, {'s1': 's1', 's2': 's2'}, {'s1': ['v1', 'v2'], 's2': ['v1', 'v2']}, {'s1': 's1', 's2': 's2'}, {1: 1, 2:2}, {'s1': 's1', 's2': 's2'}, {0.5: 0.5, 1.9: 1.9}, {'2022-06-14': '2022-06-14', '2022-06-15': '2022-06-15'}, {'2022-06-14 12:00:00': '2022-06-14 12:00:00', '2022-06-14 14:00:00': '2022-06-14 14:00:00'}, {'93fd2f47-4567-4eb8-9e7b-0e17f4c77837': '93fd2f47-4567-4eb8-9e7b-0e17f4c77837'}, {'world': 'world'}, {});
insert into 00745_merge_tree_map_data_type values(2, {'s3': 's3'}, {'s3': ['v3', 'v4']}, {'s3': 's3'}, {3: 3}, {'s3': 's3'}, {3.5: 3.5, 4.9: 4.9}, {'2022-06-16': '2022-06-16'}, {'2022-06-16 12:00:00': '2022-06-16 12:00:00'}, {'13fd2f47-4567-4eb8-9e7b-0e17f4c77837': '13fd2f47-4567-4eb8-9e7b-0e17f4c77837'}, {'hello': 'hello', 'foo': 'foo'}, {});

select '';
select 'select * :';
select * from 00745_merge_tree_map_data_type order by n;

select '';
select 'select [] :';
select string_map['s1'], string_map['s10'], string_array_map['s2'], string_array_map['s10'], fixed_string_map['s1'::FixedString(2)], fixed_string_map['s8'::FixedString(2)], int_map[1], int_map[10], lowcardinality_map['s1'], lowcardinality_map['s10'], float_map[0.5], float_map[0.4], date_map['2022-06-14'], date_map['2021-06-14'], datetime_map['2022-06-14 12:00:00'], datetime_map['2021-06-14 12:00:00'], uuid_map['93fd2f47-4567-4eb8-9e7b-0e17f4c77837'], uuid_map['53fd2f47-4567-4eb8-9e7b-0e17f4c77837'], enums_map['hello'], new_enums_map['non-exists'] from 00745_merge_tree_map_data_type order by n;

select '';
select 'select {} :';
select string_map{'s1'}, string_map{'s10'}, string_array_map{'s2'}, string_array_map{'s10'}, fixed_string_map{'s1'::FixedString(2)}, fixed_string_map{'s8'::FixedString(2)}, int_map{1}, int_map{10}, lowcardinality_map{'s1'}, lowcardinality_map{'s10'}, float_map{0.5}, float_map{0.4}, date_map{'2022-06-14'}, date_map{'2021-06-14'}, datetime_map{'2022-06-14 12:00:00'}, datetime_map{'2021-06-14 12:00:00'}, uuid_map{'93fd2f47-4567-4eb8-9e7b-0e17f4c77837'}, uuid_map{'53fd2f47-4567-4eb8-9e7b-0e17f4c77837'}, enums_map{'hello'}, new_enums_map{'non-exists'} from 00745_merge_tree_map_data_type order by n;  --skip_if_readonly_ci

select '';
select 'select mapKeys :';
select mapKeys(string_map), mapKeys(string_array_map), mapKeys(fixed_string_map), mapKeys(int_map), mapKeys(lowcardinality_map), mapKeys(float_map), mapKeys(date_map), mapKeys(datetime_map), mapKeys(uuid_map), mapKeys(enums_map) from 00745_merge_tree_map_data_type order by n;

select '';
select 'select mapValues :';
select mapValues(string_map), mapValues(string_array_map), mapValues(fixed_string_map), mapValues(int_map), mapValues(lowcardinality_map), mapValues(float_map), mapValues(date_map), mapValues(datetime_map), mapValues(uuid_map), mapValues(enums_map) from 00745_merge_tree_map_data_type order by n;

select '';
select 'select getMapKeys :';
select getMapKeys(currentDatabase(0), '00745_merge_tree_map_data_type', 'string_map'); -- { serverError 36 }

DROP TABLE 00745_merge_tree_map_data_type;

--- test BYTE map
select '--------------------------------------------------';
SELECT 'test BYTE map';
CREATE TABLE 00745_merge_tree_map_data_type (
    n UInt8,
    `string_map` Map(String, String),
    `string_array_map` Map(String, Array(String)),
    `fixed_string_map` Map(FixedString(2), FixedString(2)),
    `int_map` Map(UInt32, UInt32),
    `lowcardinality_map` Map(LowCardinality(String), LowCardinality(Nullable(String))),
    `float_map` Map(Float32, Float32),
    `date_map` Map(Date, Date),
    `datetime_map` Map(DateTime('Asia/Shanghai'), DateTime('Asia/Shanghai')),
    `uuid_map` Map(UUID, String),
    `enums_map` Map(Enum8('hello' = 0, 'world' = 1, 'foo' = -1), Int32))
Engine=CnchMergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0;

insert into 00745_merge_tree_map_data_type values(1, {'s1': 's1', 's2': 's2'}, {'s1': ['v1', 'v2'], 's2': ['v1', 'v2']}, {'s1': 's1', 's2': 's2'}, {1: 1, 2:2}, {'s1': 's1', 's2': 's2'}, {0.5: 0.5, 1.9: 1.9}, {'2022-06-14': '2022-06-14', '2022-06-15': '2022-06-15'}, {'2022-06-14 12:00:00': '2022-06-14 12:00:00', '2022-06-14 14:00:00': '2022-06-14 14:00:00'}, {'93fd2f47-4567-4eb8-9e7b-0e17f4c77837': '93fd2f47-4567-4eb8-9e7b-0e17f4c77837'}, {'world': 4});
insert into 00745_merge_tree_map_data_type values(2, {'s3': 's3'}, {'s3': ['v3', 'v4']}, {'s3': 's3'}, {3: 3}, {'s3': 's3'}, {3.5: 3.5, 4.9: 4.9}, {'2022-06-16': '2022-06-16'}, {'2022-06-16 12:00:00': '2022-06-16 12:00:00'}, {'13fd2f47-4567-4eb8-9e7b-0e17f4c77837': '13fd2f47-4567-4eb8-9e7b-0e17f4c77837'}, {'hello': 1, 'foo': 2});

select '';
select 'select * :';
select * from 00745_merge_tree_map_data_type order by n;

select '';
select 'select [] :';
select string_map['s1'], string_map['s10'], string_array_map['s2'], string_array_map['s10'], fixed_string_map['s1'::FixedString(2)], fixed_string_map['s8'::FixedString(2)], int_map[1], int_map[10], lowcardinality_map['s1'], lowcardinality_map['s10'], float_map[0.5], float_map[0.4], date_map['2022-06-14'], date_map['2021-06-14'], datetime_map['2022-06-14 12:00:00'], datetime_map['2021-06-14 12:00:00'], uuid_map['93fd2f47-4567-4eb8-9e7b-0e17f4c77837'], uuid_map['53fd2f47-4567-4eb8-9e7b-0e17f4c77837'], enums_map['hello'] from 00745_merge_tree_map_data_type order by n;

select '';
select 'select {} :';
select string_map{'s1'}, string_map{'s10'}, string_array_map{'s2'}, string_array_map{'s10'}, fixed_string_map{'s1'::FixedString(2)}, fixed_string_map{'s8'::FixedString(2)}, int_map{1}, int_map{10}, lowcardinality_map{'s1'}, lowcardinality_map{'s10'}, float_map{0.5}, float_map{0.4}, date_map{'2022-06-14'}, date_map{'2021-06-14'}, datetime_map{'2022-06-14 12:00:00'}, datetime_map{'2021-06-14 12:00:00'}, uuid_map{'93fd2f47-4567-4eb8-9e7b-0e17f4c77837'}, uuid_map{'53fd2f47-4567-4eb8-9e7b-0e17f4c77837'}, enums_map{'hello'} from 00745_merge_tree_map_data_type order by n;  --skip_if_readonly_ci

select '';
select 'select mapKeys :';
select mapKeys(string_map), mapKeys(string_array_map), mapKeys(fixed_string_map), mapKeys(int_map), mapKeys(lowcardinality_map), mapKeys(float_map), mapKeys(date_map), mapKeys(datetime_map), mapKeys(uuid_map), mapKeys(enums_map) from 00745_merge_tree_map_data_type order by n;

select '';
select 'select mapValues :';
select mapValues(string_map), mapValues(string_array_map), mapValues(fixed_string_map), mapValues(int_map), mapValues(lowcardinality_map), mapValues(float_map), mapValues(date_map), mapValues(datetime_map), mapValues(uuid_map), mapValues(enums_map) from 00745_merge_tree_map_data_type order by n;

select '';
select 'select getMapKeys :';
select getMapKeys(currentDatabase(0), '00745_merge_tree_map_data_type', 'string_map');
select getMapKeys(currentDatabase(0), '00745_merge_tree_map_data_type', 'string_array_map');
select getMapKeys(currentDatabase(0), '00745_merge_tree_map_data_type', 'fixed_string_map');
select getMapKeys(currentDatabase(0), '00745_merge_tree_map_data_type', 'int_map');
select getMapKeys(currentDatabase(0), '00745_merge_tree_map_data_type', 'lowcardinality_map');
select getMapKeys(currentDatabase(0), '00745_merge_tree_map_data_type', 'float_map');
select getMapKeys(currentDatabase(0), '00745_merge_tree_map_data_type', 'date_map');
select getMapKeys(currentDatabase(0), '00745_merge_tree_map_data_type', 'datetime_map');
select getMapKeys(currentDatabase(0), '00745_merge_tree_map_data_type', 'uuid_map');
select getMapKeys(currentDatabase(0), '00745_merge_tree_map_data_type', 'enums_map');

DROP TABLE 00745_merge_tree_map_data_type;


-- unsupported key types
CREATE TABLE 00745_merge_tree_map_data_type (a Int32, b Map(Array(String), String)) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 00745_merge_tree_map_data_type (a Int32, b Map(Array(String), String) KV) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }

select '';
select 'unsupported value types in kv map :';
CREATE TABLE 00745_merge_tree_map_data_type (a Int32, b Map(String, Array(Tuple(String, String)))) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 00745_merge_tree_map_data_type (a Int32, b Map(String, LowCardinality(String))) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }

CREATE TABLE 00745_merge_tree_map_data_type (a Int32, b Map(String, Array(Tuple(String, String))) KV) ENGINE = CnchMergeTree ORDER BY a SETTINGS index_granularity = 2;
INSERT INTO 00745_merge_tree_map_data_type VALUES (0, {'k1': [('k', 'v')]}) (1, {'k2': [('k', 'v')]}) (2, {'k3': [('k', 'v')]});
SELECT * FROM 00745_merge_tree_map_data_type ORDER BY a;
DROP TABLE 00745_merge_tree_map_data_type;

CREATE TABLE 00745_merge_tree_map_data_type (a Int32, b Map(String, LowCardinality(String)) KV) ENGINE = CnchMergeTree ORDER BY a;
INSERT INTO 00745_merge_tree_map_data_type VALUES (0, {'k1': 'v1'}) (1, {'k2': 'v2'}) (2, {'k3': 'v3'});
SELECT * FROM 00745_merge_tree_map_data_type ORDER BY a;
DROP TABLE 00745_merge_tree_map_data_type;
