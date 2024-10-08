drop table if exists test_pathcount;

create table test_pathcount
(
    `tea_app_id`   UInt32,
    `app_id`       UInt32,
    `hash_uid`     UInt64,
    `time`         UInt64,
    `event_date`   Date,
    `event`        String,
    `os_name`      String DEFAULT '',
    `device_brand` String DEFAULT ''
) ENGINE = CnchMergeTree PARTITION BY (tea_app_id, event_date) ORDER BY (tea_app_id, event, event_date, hash_uid) SAMPLE BY hash_uid;

insert into test_pathcount (tea_app_id, app_id, hash_uid, time, event_date, event, os_name, device_brand) values (283008, 98, 9901, 1619852460000, '2010-10-10', 'one', 'android', '华为'), (283008, 98, 9901, 1619852460000, '2010-10-10', 'one', 'android', '华为'), (283008, 98, 9901, 1619852460001, '2010-10-10', 'two', 'android', '华为'), (283008, 98, 9901, 1619852460001, '2010-10-10', 'two', 'android', '华为'), (283008, 98, 9901, 1619852460101, '2010-10-10', 'one', 'android', '华为'), (283008, 98, 9901, 1619852460102, '2010-10-10', 'two', 'android', '华为');

SELECT pathSplit(99, 10)(time, multiIf(event = 'one', 1, event = 'two', 2, 0) AS e, '') AS paths FROM test_pathcount WHERE (tea_app_id = 283008) and app_id = 98 GROUP BY hash_uid;
SELECT pathSplitR(99, 10)(time, multiIf(event = 'one', 1, event = 'two', 2, 0) AS e, '') AS paths FROM test_pathcount WHERE (tea_app_id = 283008) and app_id = 98 GROUP BY hash_uid;
SELECT pathSplitD(99, 10)(time, multiIf(event = 'one', 1, event = 'two', 2, 0) AS e, '') AS paths FROM test_pathcount WHERE (tea_app_id = 283008) and app_id = 98 GROUP BY hash_uid;
SELECT pathSplitRD(99, 10)(time, multiIf(event = 'one', 1, event = 'two', 2, 0) AS e, '') AS paths FROM test_pathcount WHERE (tea_app_id = 283008) and app_id = 98 GROUP BY hash_uid;

drop table test_pathcount;

