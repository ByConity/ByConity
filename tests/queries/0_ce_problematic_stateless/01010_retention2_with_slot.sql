drop table if exists test_func_retention2;

CREATE TABLE test_func_retention2 (`hash_uid` String, `server_time` Int64, `profile` String, first_day UInt8) ENGINE = MergeTree PARTITION BY toDate(server_time) ORDER BY server_time;

insert into test_func_retention2 values ('user_1', 1587545265, 'a', 0), ('user_1', 1587545268, 'A', 1), ('user_2', 1587545265, 'b', 1);
insert into test_func_retention2 values ('user_1', 1587631740, 'a', 1), ('user_1', 1587631740, 'c', 0);
insert into test_func_retention2 values ('user_2', 1587718205, 'b', 0), ('user_4', 1587718205, 'd', 1);
insert into test_func_retention2 values ('user_2', 1587805115, 'B', 1), ('user_2', 1587805113, 'd', 1), ('user_3', 1587805113, 'C', 1);
insert into test_func_retention2 values ('user_1', 1587891525, 'A', 0), ('user_4', 1587891525, 'd', 1);

insert into test_func_retention2 values ('user_1', 1587545265, 'aea', 0), ('user_1', 1587545268, 'Ad', 1), ('user_2', 1587545265, 'bd', 1);
insert into test_func_retention2 values ('user_1', 1587631740, 'avv', 1), ('user_1', 1587631740, 'cw', 0);
insert into test_func_retention2 values ('user_2', 1587718205, 'b33', 0), ('user_4', 1587718205, 'dc', 1);
insert into test_func_retention2 values ('user_2', 1587805115, 'Bgg', 1), ('user_2', 1587805113, 'de', 1), ('user_3', 1587805113, 'Cf', 1);
insert into test_func_retention2 values ('user_1', 1587891525, 'Aee', 0), ('user_4', 1587891525, 'da', 1);

insert into test_func_retention2 values ('user_1', 1587545265, 'a', 0), ('user_2', 1587545265, 'b', 1);
insert into test_func_retention2 values ('user_1', 1587631740, 'a', 1), ('user_3', 1587631740, 'c', 1);
insert into test_func_retention2 values ('user_2', 1587718205, 'b', 0), ('user_4', 1587718205, 'd', 1);
insert into test_func_retention2 values ('user_2', 1587805113, 'B', 1), ('user_3', 1587805113, 'C', 1);
insert into test_func_retention2 values ('user_1', 1587891525, 'A', 0), ('user_4', 1587891525, 'd', 1);

SELECT retention2(4,1,2,3,4)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS first_events
        FROM test_func_retention2
        WHERE first_day
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS return_events
        FROM test_func_retention2
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT retention2(4,0,0)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS first_events
        FROM test_func_retention2
        WHERE first_day
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS return_events
        FROM test_func_retention2
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT retention2(4,1,1)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS first_events
        FROM test_func_retention2
        WHERE first_day
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS return_events
        FROM test_func_retention2
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT retention2(4,2,2)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS first_events
        FROM test_func_retention2
        WHERE first_day
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS return_events
        FROM test_func_retention2
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT retention2(4)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS first_events
        FROM test_func_retention2
        WHERE first_day
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS return_events
        FROM test_func_retention2
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT retention2(4,1,2)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS first_events
        FROM test_func_retention2
        WHERE first_day
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArrayIf(4, 1587484800, 86400)(server_time, 1) AS return_events
        FROM test_func_retention2
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT '--constant--';

SELECT retention2(4,0,1,2,3)([1],[1]);
SELECT retention2(4,0,1,2,3)([1],[2]);
SELECT retention2(4,0,1,2,3)([1],[3]);
SELECT retention2(4,0,1,2,3)([1],[4]);
SELECT retention2(10, 5, 6)([246, 3], [246, 3]);
SELECT retention2(4, 1, 2)([2], [2]);
SELECT retention2(4, 1, 1)([2], [2]);
SELECT retention2(4, 0, 0)([2], [2]);
SELECT retention2(4, 0, 0)([3], [3]);
SELECT retention2(4, 0, 0, 1, 1)([3], [3]);
SELECT retention2(4, 0, 0, 1, 1, 2, 2)([3], [3]);

SELECT '--with profile--';

SELECT retention2(4, 1, 2, 3, 4)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS first_events
        FROM test_func_retention2
        WHERE (server_time % 5) = 0
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS return_events
        FROM test_func_retention2
        WHERE (server_time % 3) = 0
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT retention2(4, 1, 2)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS first_events
        FROM test_func_retention2
        WHERE (server_time % 5) = 0
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS return_events
        FROM test_func_retention2
        WHERE (server_time % 3) = 0
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT retention2(4, 0, 0)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS first_events
        FROM test_func_retention2
        WHERE (server_time % 5) = 0
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS return_events
        FROM test_func_retention2
        WHERE (server_time % 3) = 0
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT retention2(4, 1, 1)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS first_events
        FROM test_func_retention2
        WHERE (server_time % 5) = 0
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS return_events
        FROM test_func_retention2
        WHERE (server_time % 3) = 0
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT retention2(4, 2, 2)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS first_events
        FROM test_func_retention2
        WHERE (server_time % 5) = 0
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS return_events
        FROM test_func_retention2
        WHERE (server_time % 3) = 0
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT retention2(4)(first_events, return_events)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS first_events
        FROM test_func_retention2
        WHERE (server_time % 5) = 0
        GROUP BY hash_uid
    )
    LEFT JOIN
    (
        SELECT
            hash_uid,
            genArray(4, 1587484800, 86400)(server_time, profile) AS return_events
        FROM test_func_retention2
        WHERE (server_time % 3) = 0
        GROUP BY hash_uid
    ) USING (hash_uid)
);

SELECT '--others--';

SELECT retention2(2, 1, 1)(start, end)
FROM
(
    SELECT
        number AS hash_uid,
        [1] AS start
    FROM numbers(3)
) AS a
LEFT JOIN
(
    SELECT
        number + 1 AS hash_uid,
        [2] AS end
    FROM numbers(3)
) AS b ON a.hash_uid = b.hash_uid;

drop table if exists test_func_retention2;

