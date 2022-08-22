
drop table if exists table;
create table table (val Int32) engine = CnchMergeTree order by val;
insert into table values (-2), (0), (2);
select count() from table where toUInt64(val) == 0;
