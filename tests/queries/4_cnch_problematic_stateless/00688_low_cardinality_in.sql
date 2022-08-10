USE test;
drop table if exists test.lc;
create table test.lc (str StringWithDictionary, val UInt8WithDictionary) engine = CnchMergeTree order by tuple();
insert into test.lc values ('a', 1), ('b', 2);

SET enable_left_join_to_right_join=0;

select str, str in ('a', 'd') from test.lc order by str;
select val, val in (1, 3) from test.lc order by val;
select str, str in (select arrayJoin(['a', 'd'])) from test.lc order by str;
select val, val in (select arrayJoin([1, 3])) from test.lc order by  val;
select str, str in (select str from test.lc) from test.lc order by str;
select val, val in (select val from test.lc) from test.lc order by val;
drop table if exists test.lc;
