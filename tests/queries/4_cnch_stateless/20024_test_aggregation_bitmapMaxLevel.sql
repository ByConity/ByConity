drop table if exists test_maxlevel_20022;

create table if not exists test_maxlevel_20022 (tag String, uids BitMap64) Engine=CnchMergeTree order by tag;

insert into test_maxlevel_20022 values ('a', [1,2,3,4,5,6,7,8,9,10]);
insert into test_maxlevel_20022 values ('b', [1,2,3,4,5,6,7,8]);
insert into test_maxlevel_20022 values ('c', [1,2,3,4,5,6]);
insert into test_maxlevel_20022 values ('d', [1,2,3,4]);
insert into test_maxlevel_20022 values ('e', [1,2]);

select bitmapMaxLevel(level, uids)
from (
         select
             multiIf(tag='a', 1, tag='b', 2, tag='c', 3, tag='d', 4, tag='e', 5, -1) as level,
             uids
         from test_maxlevel_20022
     );

select bitmapMaxLevel(1)(level, uids)
from (
         select
             multiIf(tag='a', 1, tag='b', 2, tag='c', 3, tag='d', 4, tag='e', 5, -1) as level,
             uids
         from test_maxlevel_20022
     );

select bitmapMaxLevel(2)(level, uids)
from (
         select
             multiIf(tag='a', 1, tag='b', 2, tag='c', 3, tag='d', 4, tag='e', 5, -1) as level,
             uids
         from test_maxlevel_20022
     );

drop table if exists test_maxlevel_20022;


---- level is int in the table
create table if not exists test_maxlevel_20022 (level Int32, uids BitMap64) Engine=CnchMergeTree order by level;

insert into test_maxlevel_20022 values (-2, [1,2,3,4,5,6,7,8,9,10]);
insert into test_maxlevel_20022 values (-1, [1,2,3,4,5,6,7,8]);
insert into test_maxlevel_20022 values (1, [1,2,3,4,5,6]);
insert into test_maxlevel_20022 values (2, [1,2,3,4]);
insert into test_maxlevel_20022 values (3, [1,2]);

select bitmapMaxLevel(level, uids) from test_maxlevel_20022;

select bitmapMaxLevel(1)(level, uids)
from (
         select
             level,
             uids
         from test_maxlevel_20022
     );

select bitmapMaxLevel(2)(level, uids)
from (
         select
             level,
             uids
         from test_maxlevel_20022
     );

drop table if exists test_maxlevel_20022;