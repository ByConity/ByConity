DROP TABLE IF EXISTS test_monotonic;
CREATE TABLE test_monotonic
(
    `event_time` Int32,
    `name` Nullable(String),
    `room_id` String
)
ENGINE = CnchMergeTree
ORDER BY (event_time, room_id);

insert into test_monotonic values(1, '333', '1'),(1703045559, null, '3'),(3333444444, null, '2');

SELECT
    toStartOfInterval(toDateTime(event_time), toIntervalMinute(1), 'Asia/Shanghai'),
    name,
    room_id,
    count()
FROM test_monotonic
GROUP BY
    toStartOfInterval(toDateTime(event_time), toIntervalMinute(1), 'Asia/Shanghai'),
    name,
    room_id
ORDER BY room_id;

DROP TABLE IF EXISTS test_monotonic;
