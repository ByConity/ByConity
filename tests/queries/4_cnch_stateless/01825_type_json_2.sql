-- Tags: no-fasttest

set allow_experimental_object_type = 1;
set dialect_type = 'MYSQL';
SET block_json_query_in_optimizer = 0;

drop table if EXISTS t_json_function;
CREATE TABLE t_json_function
(
    `id` int,
    `json` JSON not null
)
ENGINE = CnchMergeTree
ORDER BY id;

INSERT INTO t_json_function VALUES (1, '{"id": 1, "obj": {"domain": "google.com", "path": "mail"}}');
INSERT INTO t_json_function VALUES (2, '{"id": 2, "obj": {"domain": "clickhouse.com", "user_id": 3222}}');
INSERT INTO t_json_function VALUES (3, '{"id": 3, "obj": {"domain": "clickhouse.com", "user_id": 3333}}');

select JSONExtractUInt(json, 'id') as json_id from t_json_function order by json_id;
select JSONExtractUInt(JSONExtractRaw(json, 'obj'), 'user_id') as user_id from t_json_function order by user_id;

drop table if EXISTS t_json_function;