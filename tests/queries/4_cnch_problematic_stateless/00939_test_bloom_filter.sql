
drop table if exists test_bloom;

create table test_bloom (date Date, vid Array(Int32) BLOOM) engine = CnchMergeTree partition by date order by date;

insert into table test_bloom values ('2019-01-01', [1]);
insert into table test_bloom values ('2019-01-02', [2]);
insert into table test_bloom values ('2019-01-03', [3]);
insert into table test_bloom values ('2019-01-04', [1, 2]);
insert into table test_bloom values ('2019-01-05', [1, 3]);

set enable_bloom_filter = 1;
set enable_range_bloom_filter = 1;

select date from test_bloom where arraySetCheck(vid, (1)) order by date;
select date from test_bloom where arraySetCheck(vid, (2)) order by date;

drop table if exists test_bloom;