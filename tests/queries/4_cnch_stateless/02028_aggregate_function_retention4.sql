DROP TABLE IF EXISTS test_func_retention4;

CREATE TABLE test_func_retention4 (`hash_uid` String, `server_time` UInt64, first_day UInt8) Engine = CnchMergeTree PARTITION BY toDate(server_time) ORDER BY server_time;

INSERT INTO test_func_retention4 VALUES ('user_1', 1587545265, 0), ('user_2', 1587545265, 1);
INSERT INTO test_func_retention4 VALUES ('user_1', 1587631740, 1), ('user_3', 1587631740, 1);
INSERT INTO test_func_retention4 VALUES ('user_2', 1587718205, 0), ('user_4', 1587718205, 1);
INSERT INTO test_func_retention4 VALUES ('user_2', 1587805113, 1), ('user_3', 1587805113, 1);
INSERT INTO test_func_retention4 VALUES ('user_1', 1587891525, 0), ('user_4', 1587891525, 1);

SELECT arrayJoin(retention4(10, '2020-04-22', '2020-04-25')(first_events, first_events)) FROM (SELECT hash_uid, genArray(10, 1587484800, 86400)(server_time) AS first_events FROM test_func_retention4 WHERE first_day GROUP BY hash_uid);
SELECT '';
SELECT arrayJoin(retention4(10, '2020-04-22', '2020-04-25')(first_events, first_events)) FROM (SELECT hash_uid, genArray(10, 1587484800, 86400)(server_time) AS first_events FROM test_func_retention4 GROUP BY hash_uid);
SELECT '';
SELECT arrayJoin(retention4(10, '2020-04-22', '2020-04-20')(first_events, first_events)) FROM (SELECT hash_uid, genArray(10, 1587484800, 86400)(server_time) AS first_events FROM test_func_retention4 WHERE first_day GROUP BY hash_uid);  -- { serverError 457 }
SELECT arrayJoin(retention4(10, '2020-04-22', '2020-04-25')(first_events, return_events)) FROM (SELECT * FROM (SELECT hash_uid, genArray(10, 1587484800, 86400)(server_time) AS first_events FROM test_func_retention4 WHERE first_day GROUP BY hash_uid) AS table_a LEFT JOIN (SELECT hash_uid, genArray(10, 1587484800, 86400)(server_time) AS return_events FROM test_func_retention4 GROUP BY hash_uid) AS table_b USING hash_uid);
DROP TABLE test_func_retention4;
