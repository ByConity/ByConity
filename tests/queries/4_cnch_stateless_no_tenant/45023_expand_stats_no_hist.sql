set enable_optimizer=1;
set create_stats_time_output=0;
set statistics_current_timestamp=1715602800;
set statistics_collect_histogram=0;
drop table if exists tb;
drop table if exists tb_local;

create table tb (
    `id` UInt64,
    `date` Date,
    `date32` Date32,
    `datetime` DateTime,
    `datetime64` DateTime64(3),
    `old_date` Date,
    `old_date32` Date32,
    `old_datetime` DateTime,
    `old_datetime64` DateTime64(3)
) ENGINE = CnchMergeTree()
order by
    id;

insert into
    tb
select
    number,
    toDate('2024-05-13') - (number + 10),
    toDate('2024-05-13') - (number + 10),
    toDateTime('2024-05-13 15:20:00') - toIntervalDay(10 + number),
    toDateTime('2024-05-13 15:20:00') - toIntervalDay(10 + number),
    toDate('2020-01-01') - (number + 10),
    toDate('2020-01-01') - (number + 10),
    toDateTime('2020-01-01 00:00:00') - (number + 10),
    toDateTime('2020-01-01 00:00:00') - (number + 10)
from
    system.numbers
limit
    100;

insert into tb select * from tb;
insert into tb select * from tb;
insert into tb select * from tb;

create stats tb;
show stats tb;
explain stats=1, verbose=0 select id from tb where date > toDate('2024-05-13') - 5;
explain stats=1, verbose=0 select id from tb where date = toDate('2024-05-13');
explain stats=1, verbose=0 select id from tb where date32 > toDate('2024-05-13') - 5;
explain stats=1, verbose=0 select id from tb where date32 = toDate('2024-05-13');
explain stats=1, verbose=0 select id from tb where datetime > toDateTime('2024-05-13 15:20:00') - toIntervalDay(5);
explain stats=1, verbose=0 select id from tb where datetime = toDateTime('2024-05-13 15:20:00');
explain stats=1, verbose=0 select id from tb where datetime64 > toDateTime('2024-05-13 15:20:00') - toIntervalDay(5);
explain stats=1, verbose=0 select id from tb where datetime64 = toDateTime('2024-05-13 15:20:00');
explain stats=1, verbose=0 select id from tb where old_date > toDate('2024-05-13') - 5;
explain stats=1, verbose=0 select id from tb where old_date32 > toDate('2024-05-13') - 5;
explain stats=1, verbose=0 select id from tb where old_datetime > toDateTime('2024-05-13 15:20:00') - toIntervalDay(5);
explain stats=1, verbose=0 select id from tb where old_datetime64 > toDateTime('2024-05-13 15:20:00') - toIntervalDay(5);



set statistics_expand_to_current=1;
show stats tb;
explain stats=1, verbose=0 select id from tb where date > toDate('2024-05-13') - 5;
explain stats=1, verbose=0 select id from tb where date = toDate('2024-05-13');
explain stats=1, verbose=0 select id from tb where date32 > toDate('2024-05-13') - 5;
explain stats=1, verbose=0 select id from tb where date32 = toDate('2024-05-13');
explain stats=1, verbose=0 select id from tb where datetime > toDateTime('2024-05-13 15:20:00') - toIntervalDay(5);
explain stats=1, verbose=0 select id from tb where datetime = toDateTime('2024-05-13 15:20:00');
explain stats=1, verbose=0 select id from tb where datetime64 > toDateTime('2024-05-13 15:20:00') - toIntervalDay(5);
explain stats=1, verbose=0 select id from tb where datetime64 = toDateTime('2024-05-13 15:20:00');
explain stats=1, verbose=0 select id from tb where old_date > toDate('2024-05-13') - 5;
explain stats=1, verbose=0 select id from tb where old_date32 > toDate('2024-05-13') - 5;
explain stats=1, verbose=0 select id from tb where old_datetime > toDateTime('2024-05-13 15:20:00') - toIntervalDay(5);
explain stats=1, verbose=0 select id from tb where old_datetime64 > toDateTime('2024-05-13 15:20:00') - toIntervalDay(5);

