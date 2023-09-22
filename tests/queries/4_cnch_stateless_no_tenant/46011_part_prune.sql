DROP TABLE IF EXISTS events;
CREATE TABLE events (`app_id` UInt32, `event_date` Date, `hash_uid` UInt64, `event` String, `time` UInt64) ENGINE = CnchMergeTree PARTITION BY (`app_id`, `event_date`) CLUSTER BY cityHash64(`hash_uid`) INTO 4 BUCKETS ORDER BY (`event`, `hash_uid`, `time`) SAMPLE BY `hash_uid`;

insert into events values (1, '2023-01-01', 1, 'da', 1);
insert into events values (2, '2023-01-01', 1, 'da', 1);
insert into events values (3, '2023-01-01', 1, 'da', 1);
insert into events values (4, '2023-01-01', 1, 'da', 1);

select * from events where app_id > '4';
