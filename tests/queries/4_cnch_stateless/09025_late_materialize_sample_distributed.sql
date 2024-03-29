create table if not exists sample_prewhere (date Date, id Int32, time Int64) engine = CnchMergeTree partition by date order by (id, time, intHash64(time)) sample by intHash64(time) settings enable_late_materialize = 1;

insert into sample_prewhere values ('2019-01-01', 2, toDateTime('2019-07-20 00:00:01'));
insert into sample_prewhere values ('2019-01-01', 1, toDateTime('2019-07-20 00:00:02'));
insert into sample_prewhere values ('2019-01-02', 3, toDateTime('2019-07-20 00:00:03'));

select id from sample_prewhere SAMPLE 1 where toDateTime(time) = '2019-07-20 00:00:00';

drop table sample_prewhere;
