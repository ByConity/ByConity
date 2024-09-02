set dialect_type = 'MYSQL';
SELECT '--JSON_VALUE--';
SELECT JSON_VALUE('{"hello":1}'::Object('json'), '$'); -- root is a complex object => default value (empty string)
SELECT JSON_VALUE('{"hello":1}'::Object('json'), '$.hello');
SELECT JSON_VALUE('{"hello":1.2}'::Object('json'), '$.hello');
SELECT JSON_VALUE('{"hello":true}'::Object('json'), '$.hello');
SELECT JSON_VALUE('{"hello":"world"}'::Object('json'), '$.hello');
SELECT JSON_VALUE('{"hello":null}'::Object('json'), '$.hello');
SELECT JSON_VALUE('{"hello":["world","world2"]}'::Object('json'), '$.hello');
SELECT JSON_VALUE('{"hello":{"world":"!"}}'::Object('json'), '$.hello');

SELECT '--JSON_QUERY--';
-- SELECT JSON_QUERY('{"hello":1}'::Object('json'), '$');
-- SELECT JSON_QUERY('{"hello":1}'::Object('json'), '$.hello');
-- SELECT JSON_QUERY('{"hello":1.2}'::Object('json'), '$.hello');
-- SELECT JSON_QUERY('{"hello":true}'::Object('json'), '$.hello');
-- SELECT JSON_QUERY('{"hello":"world"}'::Object('json'), '$.hello');
-- SELECT JSON_QUERY('{"hello":null}'::Object('json'), '$.hello');
-- SELECT JSON_QUERY('{"hello":["world","world2"]}'::Object('json'), '$.hello');
-- SELECT JSON_QUERY('{"hello":{"world":"!"}}'::Object('json'), '$.hello');
-- SELECT JSON_QUERY( '{hello:{"world":"!"}}}'::Object('json'), '$.hello'); -- invalid json => default value (empty string)
-- SELECT JSON_QUERY('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}'::Object('json'), '$.array[*][0 to 2, 4]');

SELECT '--JSON_EXISTS--';
SELECT JSON_EXISTS('{"hello":1}'::Object('json'), '$');
SELECT JSON_EXISTS('{"hello":1}'::Object('json'), '$.hello');
SELECT JSON_EXISTS('{"hello":1,"world":2}'::Object('json'), '$.world');
SELECT JSON_EXISTS('{"hello":{"world":1}}'::Object('json'), '$.world');
SELECT JSON_EXISTS('{"hello":{"world":1}}'::Object('json'), '$.hello.world');
SELECT JSON_EXISTS('{"hello":["world"]}'::Object('json'), '$.hello[*]');
SELECT JSON_EXISTS('{"hello":["world"]}'::Object('json'), '$.hello[0]');
SELECT JSON_EXISTS('{"hello":["world"]}'::Object('json'), '$.hello[1]');
SELECT JSON_EXISTS('{"a":[{"b":1},{"c":2}]}'::Object('json'), '$.a[*].b');
SELECT JSON_EXISTS('{"a":[{"b":1},{"c":2}]}'::Object('json'), '$.a[*].f');
SELECT JSON_EXISTS('{"a":[[{"b":1}, {"g":1}],[{"h":1},{"y":1}]]}'::Object('json'), '$.a[*][0].h');

SELECT '--JSON_LENGTH--';
-- SELECT JSON_LENGTH('[1, 2, {"a": 3}]'::Object('json'), '$');
SELECT JSON_LENGTH('{"a": 1, "b": {"c": 30}}'::Object('json'), '$');
SELECT JSON_LENGTH('{"a": 1, "b": {"c": 30}}'::Object('json'), '$.b');
SELECT JSON_LENGTH('{"hello":["world"]}'::Object('json'), '$.hello[*]');
SELECT JSON_LENGTH('{"hello":["world","world2"]}'::Object('json'), '$.hello');
SELECT JSON_LENGTH('{"hello":{"world":"!"}}'::Object('json'), '$.hello');
-- SELECT JSON_LENGTH( '{hello:{"world":"!"}}}'::Object('json'), '$.hello'); -- invalid json => default value (empty string)
SELECT JSON_LENGTH('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}'::Object('json'), '$.array[*][0 to 2, 4]');

-- SELECT '--JSON_VALID--';
-- SELECT JSON_VALID('{"name": "John Doe", "age": 30}'::Object('json'));
-- SELECT JSON_VALID('{"name: "John Doe", "age": 30}'::Object('json'));
-- SELECT JSON_VALID('{"name": 123, "age": 30}'::Object('json'));
-- SELECT JSON_VALID('{"name": "John Doe"}'::Object('json'));
-- SELECT JSON_VALID(NULL);

SELECT '--JSON_EXTRACT--';
SELECT JSON_EXTRACT('{"hello":1}'::Object('json'), '$.hello');
SELECT JSON_EXTRACT('{"name": "John Doe", "age": 30}'::Object('json'), '$.name');
SELECT JSON_EXTRACT('{"person": {"name": "John Doe", "age": 30}}'::Object('json'), '$.person.name');
SELECT JSON_EXTRACT('{"colors": ["red", "green", "blue"]}'::Object('json'), '$.colors[1]');
SELECT JSON_EXTRACT('{"name": "John Doe", "age": 30}'::Object('json'), '$.address');
-- SELECT JSON_EXTRACT('{"name": "John Doe", "age": 30}'::Object('json'), '$.name');
SELECT json_extract('{"a":[10, 20, [30, 40]]}'::Object('json'), '$.a.1')

SELECT '--JSON_CONTAINS--';
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), 'hello', '$.a');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), -100, '$.b[0]');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), -100, '$.b');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), -100, '$.b[*]');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), 'hello');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), 'world', '$.a');
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), -1, '$.b[0]');
select JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}'::Object('json'), '{"a":1, "c": {"d": 4}}'::Object('json'));
SELECT JSON_CONTAINS('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), [-100, 200.0], '$.b') settings parse_literal_as_decimal = 0;

SELECT '--JSON_CONTAINS_PATH--';
SELECT JSON_CONTAINS_PATH('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), 'all', '$.a');
SELECT JSON_CONTAINS_PATH('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), 'all', '$.a', '$.b');
SELECT JSON_CONTAINS_PATH('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), 'one', '$.a', '$.c');
SELECT JSON_CONTAINS_PATH('{"a": "hello", "b": [-100, 200.0, 300]}'::Object('json'), 'all', '$.a', '$.c');

SELECT '--JSON_KEYS--';
SELECT json_keys('{"a": 1, "b": {"c": 30}}'::Object('json'),'$.b');
SELECT JSON_KEYS('{"a": 1, "b": {"c": 30}}'::Object('json'));

SELECT '--JSON_SIZE--';
SELECT json_size('{"x":{"a":1, "b": 2}}'::Object('json'), '$.x');
SELECT json_size('{"x": {"a": 1, "b": 2}}'::Object('json'), '$.x.a');

-- SELECT '--MANY ROWS--';
-- DROP TABLE IF EXISTS 01889_sql_json;
-- set allow_experimental_object_type = 1;
-- set enable_optimizer = 0;
-- CREATE TABLE 01889_sql_json (id UInt8, json Object('json')) ENGINE = MergeTree ORDER BY id;
-- INSERT INTO 01889_sql_json(id, json) VALUES(0, '{"name":"Ivan","surname":"Ivanov","friends":["Vasily","Kostya","Artyom"]}');
-- INSERT INTO 01889_sql_json(id, json) VALUES(1, '{"name":"Katya","surname":"Baltica","friends":["Tihon","Ernest","Innokentiy"]}');
-- INSERT INTO 01889_sql_json(id, json) VALUES(2, '{"name":"Vitali","surname":"Brown","friends":["Katya","Anatoliy","Ivan","Oleg"]}');
-- SELECT id, JSON_QUERY(json, '$.friends[0 to 2]') FROM 01889_sql_json ORDER BY id;
-- DROP TABLE 01889_sql_json;
