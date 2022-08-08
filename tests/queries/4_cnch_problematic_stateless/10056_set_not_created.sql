DROP TABLE IF EXISTS test.oner_event_test;
CREATE TABLE test.oner_event_test (`date` String, `user_id` String) Engine = CnchMergeTree Partition By toDate(date) Order by (user_id, toDate(date));

INSERT INTO test.oner_event_test VALUES ('2021-08-30', 'a'), ('2021-09-01', 'b');

SELECT count() FROM test.oner_event_test WHERE date IN ('2021-08-30');

DROP TABLE IF EXISTS test.oner_event_test;