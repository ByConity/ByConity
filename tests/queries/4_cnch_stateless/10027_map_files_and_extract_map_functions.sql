DROP TABLE IF EXISTS 10027_map_files_and_extract_map_functions;
CREATE TABLE 10027_map_files_and_extract_map_functions ( i Int, d Date, ms Map(String, String), mi Map(Int, Int), mf Map(Float, Float), md Map(Date, Date)) ENGINE = CnchMergeTree ORDER BY i PARTITION BY d;

SELECT 'CASE 0';
INSERT INTO 10027_map_files_and_extract_map_functions VALUES (0, 0, {'k1': 'v1'}, {}, {}, {});
SELECT extractMapColumn(''), extractMapColumn('0'), extractMapColumn('__M__1.bin'), extractMapColumn('__M__1.bin1');
SELECT arrayJoin(_part_map_files as fs) as f FROM 10027_map_files_and_extract_map_functions ORDER BY f;
SELECT arrayJoin(_map_column_keys as fs) as f FROM 10027_map_files_and_extract_map_functions ORDER BY f SETTINGS early_limit_for_map_virtual_columns = 1, max_threads = 1;

SELECT 'CASE 1';
INSERT INTO 10027_map_files_and_extract_map_functions VALUES (0, 0, {}, {}, {}, {});
SELECT DISTINCT arrayJoin(_part_map_files as fs) as f FROM 10027_map_files_and_extract_map_functions ORDER BY f SETTINGS early_limit_for_map_virtual_columns = 1, max_threads = 1;
SELECT DISTINCT arrayJoin(_map_column_keys as fs) as f FROM 10027_map_files_and_extract_map_functions ORDER BY f SETTINGS early_limit_for_map_virtual_columns = 1, max_threads = 1;

SELECT 'CASE 2';
INSERT INTO 10027_map_files_and_extract_map_functions VALUES (0, 0, {'k1': 'v1'}, {1: 1}, {1.: 1.0}, {'2020-10-10': '2020-10-10'});
INSERT INTO 10027_map_files_and_extract_map_functions VALUES (0, 0, {'k1': 'v1'}, {1: 1}, {1.23456789: 1.0}, {});
INSERT INTO 10027_map_files_and_extract_map_functions VALUES (0, 0, {'k''2': 'v2'}, {2: 2}, {1.0: 1.0}, {});
SELECT DISTINCT arrayJoin(_part_map_files as fs) as f FROM 10027_map_files_and_extract_map_functions ORDER BY f SETTINGS early_limit_for_map_virtual_columns = 1, max_threads = 1;
SELECT DISTINCT arrayJoin(_map_column_keys as fs) as f FROM 10027_map_files_and_extract_map_functions ORDER BY f SETTINGS early_limit_for_map_virtual_columns = 1, max_threads = 1;

SELECT 'CASE 3';
SELECT f, any(extractMapColumn(f)), any(extractMapKey(f))
FROM (
    SELECT arrayJoin(_part_map_files as fs) as f FROM 10027_map_files_and_extract_map_functions SETTINGS early_limit_for_map_virtual_columns = 1, max_threads = 1
) GROUP BY f ORDER BY f;

SELECT 'CASE 4';
SELECT
    extractMapColumn(f) as m,
    arraySort(groupUniqArray(extractMapKey(f) as k )) ks
FROM (
    SELECT arrayJoin(_part_map_files as fs) as f FROM 10027_map_files_and_extract_map_functions SETTINGS early_limit_for_map_virtual_columns = 1, max_threads = 1
)
WHERE m != ''
GROUP BY m ORDER BY m;

SELECT 'CASE 5';
SELECT DISTINCT t.1 as c, t.2 as k FROM
(
SELECT arrayJoin(_map_column_keys as ts) as t FROM 10027_map_files_and_extract_map_functions SETTINGS early_limit_for_map_virtual_columns = 1
)
ORDER BY c, k;

SELECT 'CASE 6';
SELECT getMapKeys(currentDatabase(0), '10027_map_files_and_extract_map_functions', 'ms');
SELECT getMapKeys(currentDatabase(0), '10027_map_files_and_extract_map_functions', 'ms', '');

SELECT 'CASE 7';
SELECT getMapKeys(currentDatabase(0), '10027_map_files_and_extract_map_functions', 'ms', '20000101');

SELECT 'CASE 8';
INSERT INTO 10027_map_files_and_extract_map_functions VALUES (0, toDate('2023-03-03'), {'k3': 'v3'}, {}, {}, {});
SELECT getMapKeys(currentDatabase(0), '10027_map_files_and_extract_map_functions', 'ms', '20230303');
