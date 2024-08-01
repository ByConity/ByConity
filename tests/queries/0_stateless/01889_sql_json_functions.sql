set dialect_type = 'MYSQL';
SELECT '--JSON_VALUE--';
SELECT JSON_VALUE('{"hello":1}', '$'); -- root is a complex object => default value (empty string)
SELECT JSON_VALUE('{"hello":1}', '$.hello');
SELECT JSON_VALUE('{"hello":1.2}', '$.hello');
SELECT JSON_VALUE('{"hello":true}', '$.hello');
SELECT JSON_VALUE('{"hello":"world"}', '$.hello');
SELECT JSON_VALUE('{"hello":null}', '$.hello');
SELECT JSON_VALUE('{"hello":["world","world2"]}', '$.hello');
SELECT JSON_VALUE('{"hello":{"world":"!"}}', '$.hello');
SELECT JSON_VALUE('{hello:world}', '$.hello'); -- invalid json => default value (empty string)
SELECT JSON_VALUE('', '$.hello');

SELECT '--JSON_QUERY--';
SELECT JSON_QUERY('{"hello":1}', '$');
SELECT JSON_QUERY('{"hello":1}', '$.hello');
SELECT JSON_QUERY('{"hello":1.2}', '$.hello');
SELECT JSON_QUERY('{"hello":true}', '$.hello');
SELECT JSON_QUERY('{"hello":"world"}', '$.hello');
SELECT JSON_QUERY('{"hello":null}', '$.hello');
SELECT JSON_QUERY('{"hello":["world","world2"]}', '$.hello');
SELECT JSON_QUERY('{"hello":{"world":"!"}}', '$.hello');
SELECT JSON_QUERY( '{hello:{"world":"!"}}}', '$.hello'); -- invalid json => default value (empty string)
SELECT JSON_QUERY('', '$.hello');
SELECT JSON_QUERY('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}', '$.array[*][0 to 2, 4]');

SELECT '--JSON_EXISTS--';
SELECT JSON_EXISTS('{"hello":1}', '$');
SELECT JSON_EXISTS('', '$');
SELECT JSON_EXISTS('{}', '$');
SELECT JSON_EXISTS('{"hello":1}', '$.hello');
SELECT JSON_EXISTS('{"hello":1,"world":2}', '$.world');
SELECT JSON_EXISTS('{"hello":{"world":1}}', '$.world');
SELECT JSON_EXISTS('{"hello":{"world":1}}', '$.hello.world');
SELECT JSON_EXISTS('{hello:world}', '$.hello'); -- invalid json => default value (zero integer)
SELECT JSON_EXISTS('', '$.hello');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[*]');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[0]');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[1]');
SELECT JSON_EXISTS('{"a":[{"b":1},{"c":2}]}', '$.a[*].b');
SELECT JSON_EXISTS('{"a":[{"b":1},{"c":2}]}', '$.a[*].f');
SELECT JSON_EXISTS('{"a":[[{"b":1}, {"g":1}],[{"h":1},{"y":1}]]}', '$.a[*][0].h');

SELECT '--JSON_LENGTH--';
SELECT JSON_LENGTH('[1, 2, {"a": 3}]', '$');
SELECT JSON_LENGTH('{"a": 1, "b": {"c": 30}}', '$');
SELECT JSON_LENGTH('{"a": 1, "b": {"c": 30}}', '$.b');
SELECT JSON_LENGTH('{"hello":["world"]}', '$.hello[*]');
SELECT JSON_LENGTH('{"hello":["world","world2"]}', '$.hello');
SELECT JSON_LENGTH('{"hello":{"world":"!"}}', '$.hello');
SELECT JSON_LENGTH( '{hello:{"world":"!"}}}', '$.hello'); -- invalid json => default value (empty string)
SELECT JSON_LENGTH('', '$.hello');
SELECT JSON_LENGTH('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}', '$.array[*][0 to 2, 4]');

-- SELECT '--JSON_VALID--';
-- SELECT JSON_VALID('{"name": "John Doe", "age": 30}');
-- SELECT JSON_VALID('{"name: "John Doe", "age": 30}');
-- SELECT JSON_VALID('{"name": 123, "age": 30}');
-- SELECT JSON_VALID('{"name": "John Doe"}');
-- SELECT JSON_VALID(NULL);

SELECT '--JSON_EXTRACT--';
SELECT JSON_EXTRACT('{"hello":1}', '$.hello');
SELECT JSON_EXTRACT('{"name": "John Doe", "age": 30}', '$.name');
SELECT JSON_EXTRACT('{"person": {"name": "John Doe", "age": 30}}', '$.person.name');
SELECT JSON_EXTRACT('{"colors": ["red", "green", "blue"]}', '$.colors[1]');
SELECT JSON_EXTRACT('{"name": "John Doe", "age": 30}', '$.address');
SELECT JSON_EXTRACT('{"name: "John Doe", "age": 30}', '$.name');
SELECT json_extract('[10, 20, [30, 40]]', '$.[1]'); 
SELECT json_extract('[10, 20, [30, 40]]', '$.1');
SELECT json_extract('{"a":[10, 20, [30, 40]]}', '$.a.1');

SELECT '--JSON_CONTAINS--';
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}', '"hello"', '$.a');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}', '-100', '$.b[0]');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}', '-100', '$.b');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}', '-100', '$.b[*]');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}', '"hello"');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}', '"world"', '$.a');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}', '-1', '$.b[0]');
select JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}', '{"a":1, "c": {"d": 4}}');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}', '[-100, 200.0]', '$.b');

SELECT '--JSON_CONTAINS_PATH--';
SELECT JSON_CONTAINS_PATH('{"a": "hello", "b": [-100, 200.0, 300]}', 'all', '$.a');
SELECT JSON_CONTAINS_PATH('{"a": "hello", "b": [-100, 200.0, 300]}', 'all', '$.a', '$.b');
SELECT JSON_CONTAINS_PATH('{"a": "hello", "b": [-100, 200.0, 300]}', 'one', '$.a', '$.c');
SELECT JSON_CONTAINS_PATH('{"a": "hello", "b": [-100, 200.0, 300]}', 'all', '$.a', '$.c');

SELECT '--JSON_KEYS--';
SELECT json_keys('{"a": 1, "b": {"c": 30}}','$.b');
SELECT JSON_KEYS('{"a": 1, "b": {"c": 30}}');

SELECT '--JSON_SIZE--';
SELECT json_size('{"x":{"a":1, "b": 2}}', '$.x');
SELECT json_size('{"x": {"a": 1, "b": 2}}', '$.x.a');

SELECT '--JSON_ARRAY_CONTAINS--';
SELECT json_array_contains('[1, 2, 3]', 2);
SELECT json_array_contains('[1, 2, 3]', 10);
SELECT json_array_contains("['1', '2', '3']", '3');

SELECT '--JSON_ARRAY_LENGTH--';
SELECT json_array_length('[1, 2, 3]');

SELECT '--MANY ROWS--';
DROP TABLE IF EXISTS 01889_sql_json;
CREATE TABLE 01889_sql_json (id UInt8, json String) ENGINE = MergeTree ORDER BY id;
INSERT INTO 01889_sql_json(id, json) VALUES(0, '{"name":"Ivan","surname":"Ivanov","friends":["Vasily","Kostya","Artyom"]}');
INSERT INTO 01889_sql_json(id, json) VALUES(1, '{"name":"Katya","surname":"Baltica","friends":["Tihon","Ernest","Innokentiy"]}');
INSERT INTO 01889_sql_json(id, json) VALUES(2, '{"name":"Vitali","surname":"Brown","friends":["Katya","Anatoliy","Ivan","Oleg"]}');
SELECT id, JSON_QUERY(json, '$.friends[0 to 2]') FROM 01889_sql_json ORDER BY id;
DROP TABLE 01889_sql_json;
