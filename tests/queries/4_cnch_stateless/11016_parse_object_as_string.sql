DROP TABLE IF EXISTS test_parse_json_object_as_strings;

CREATE TABLE test_parse_json_object_as_strings (obj String, params Array(String)) ENGINE = CnchMergeTree() ORDER BY tuple();

SELECT 'Setting input_format_json_read_objects_as_strings should be disabled by default';
SELECT value FROM system.settings WHERE name = 'input_format_json_read_objects_as_strings';

SELECT 'INSERT with setting input_format_json_read_objects_as_strings = 1';
INSERT INTO test_parse_json_object_as_strings FORMAT JSONEachRow SETTINGS input_format_json_read_objects_as_strings = 1 {"obj":{"name":"durian"}, "params":[{"property0":"value0"},{"property1":"value1"}]};

SELECT * FROM test_parse_json_object_as_strings FORMAT Vertical;

DROP TABLE test_parse_json_object_as_strings;