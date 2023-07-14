ests/queries/0_stateless/use test;
drop table if exists test_func_retention_loss;

CREATE TABLE test_func_retention_loss (`hash_uid` String, `server_time` Int64, `profile` String, first_day UInt8) ENGINE = CnchMergeTree() PARTITION BY toDate(server_time) ORDER BY server_time;

insert into test_func_retention_loss values ('user_1', 1587545265, 'a', 0), ('user_1', 1587545268, 'A', 1), ('user_2', 1587545265, 'b', 1);
insert into test_func_retention_loss values ('user_1', 1587631740, 'a', 1), ('user_1', 1587631740, 'c', 0);
insert into test_func_retention_loss values ('user_2', 1587718205, 'b', 0), ('user_4', 1587718205, 'd', 1);
insert into test_func_retention_loss values ('user_2', 1587805115, 'B', 1), ('user_2', 1587805113, 'd', 1), ('user_3', 1587805113, 'C', 1);
insert into test_func_retention_loss values ('user_1', 1587891525, 'A', 0), ('user_4', 1587891525, 'd', 1);

-- Cannot handle Null itself, need to add `assumeNotNull`
SELECT retentionLoss(10)(assumeNotNull(first_events), assumeNotNull(return_events))
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArrayIf(10, 1587484800, 86400)(server_time, 1) AS first_events
        FROM test_func_retention_loss
        WHERE (server_time % 3) = 0
        GROUP BY hash_uid
    ) AS a
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArrayIf(10, 1587484800, 86400)(server_time, 1) AS return_events
        FROM test_func_retention_loss
        WHERE (server_time % 5) = 0
        GROUP BY hash_uid
    ) AS b USING (hash_uid)
);
SELECT retentionLoss(10)(assumeNotNull(first_events), assumeNotNull(return_events))
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArray(10, 1587484800, 86400)(server_time, profile) AS first_events
        FROM test_func_retention_loss
        WHERE (server_time % 3) = 0
        GROUP BY hash_uid
    ) AS a
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArray(10, 1587484800, 86400)(server_time, profile) AS return_events
        FROM test_func_retention_loss
        WHERE (server_time % 5) = 0
        GROUP BY hash_uid
    ) AS b USING (hash_uid)
);
SELECT retentionLoss(10)(assumeNotNull(first_events), assumeNotNull(return_events))
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArray(10, 1587484800, 86400)(server_time, server_time) AS first_events
        FROM test_func_retention_loss
        WHERE (server_time % 3) = 0
        GROUP BY hash_uid
    ) AS a
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArray(10, 1587484800, 86400)(server_time, server_time) AS return_events
        FROM test_func_retention_loss
        WHERE (server_time % 5) = 0
        GROUP BY hash_uid
    ) AS b USING (hash_uid)
);