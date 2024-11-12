SELECT get_json_object('{"n_s" : [{"ac":"abc","xz":"xz"}, {"def":"def"}], "n_i" : [1, 23]}', '$.n_s[0].ac');
SELECT get_json_object('{"n_s" : [{"ac":"abc","xz":"xz"}, {"def":"def"}], "n_i" : [1, 23]}', '$.n_s[0]');
SELECT get_json_object('{"n_s" : [{"ac":"abc","xz":"xz"}, {"def":"def"}], "n_i" : [1, 23]}', '$.n_s');
SELECT JSONExtractRaw('{"n_s" : [{"ac":"abc","xz":"xz"}, {"def":"def"}], "n_i" : [1, 23]}', 'n_s', 1, 'ac');
SELECT JSONExtractRaw('{"n_s" : [{"ac":"abc","xz":"xz"}, {"def":"def"}], "n_i" : [1, 23]}', 'n_s', 1);
SELECT JSONExtractRaw('{"n_s" : [{"ac":"abc","xz":"xz"}, {"def":"def"}], "n_i" : [1, 23]}', 'n_s');

select get_json_object('{"a":100}'::Nullable(String), '$.a');
select get_json_object('{"a":100}'::LowCardinality(Nullable(String)), '$.a');

select get_json_object(null::Nullable(String), '$.a');
select get_json_object(null::LowCardinality(Nullable(String)), '$.a');

DROP TABLE IF EXISTS test;
CREATE TABLE test(a Nullable(String)) ENGINE = CnchMergeTree ORDER BY tuple() PARTITION BY tuple();
SELECT get_json_object('{"test": "test"}', a) FROM test;
INSERT INTO test values('{"d" : "2017-08-31 18:36:48", "t" : {"a": "b"}}'), ('{"d" : "1504193808", "t" : -1}'), ('{"d" : 1504193808, "t" : ["a", "c"]}'), ('{"d" : false, "t" : [{"a":"c"}, {}]}'), ('{"d" : 01504193808, "t" : [{"a":"c"}, {}]}'), (null);
SELECT get_json_object(a, '$.t'), get_json_object(a, '$.d') FROM test;
DROP TABLE test;
