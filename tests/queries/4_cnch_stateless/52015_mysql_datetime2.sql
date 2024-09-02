set dialect_type='MYSQL';
select 'MYSQL';
select toDateTime('2012-1-2 1:2:3');
select toDateTime('20120102010203');
select toDateTime64('2012-1-2 1:2:3.123');
select toDateTime64('20120102010203.123');
select toTimeType('1:2:3.123');
select toTimeType('1:2:3.12345678');
set dialect_type='CLICKHOUSE';
select 'CLICKHOUSE';
select toDateTime('2012-1-2 1:2:3');
select toDateTime('20120102010203');
select toDateTime('20120102');
select toDateTime('1709089318');
select toDateTime('2012/1/2 1:2:3');
select toDateTime64('2012-1-2 1:2:3.123');
select toDateTime64('20120102010203.123');
select toDateTime64('2012/1/2 1:2:3');
select toDateTime64('1708658139779');
select toDateTime64('20120102');

select toDateTime('20121221010203.123');
select toDateTime('2012-12-21 01:02:03.123');
select toDateTime('2012-12-21 01:02:');
select toDateTime('2012-12-21 01::');
select toDateTime('2012-12-21');

-- Illegal Input
select toDateTime('abcd');  -- { serverError 41 }
select toDateTime('2011');  -- { serverError 41 }
select toDateTime('     2011    ----- 00001');  -- { serverError 6 }
select toDateTime('2012----0001----0002 00001:00002:00003');  -- { serverError 6 }
select toDateTime('2012----0001----0002 00001:00002');  -- { serverError 6 }
select time('256:256:256');  -- { serverError 6 }

select 'Test Insert';
drop table if exists date_test;
create table date_test (id Int32, d1 DateTime, t Time(3), d2 DateTime64) engine=CnchMergeTree order by id;
insert into date_test values(1, '20121221010203', '010203', '20121221010203.123'), (2, '20121221', '1:2:3', '2012-1-2 1:2:3.123'), (3, '1111111111.123', '123', '1709878703.123'), (4, '2012-1-2 1:2:3', 123, '2012-1-2'), (5, '20121221010203.1234', '1:2:3.123', '20121221010203.1234567');
select * from date_test;
drop table if exists date_test;
