DROP TABLE IF EXISTS tab;
set min_block_size = 1;
create table tab (A Int64) Engine=CnchMergeTree order by tuple() SETTINGS min_bytes_for_wide_part = 0;
insert into tab select cityHash64(number) from numbers(1000);
select sum(sleep(0.1)) from tab settings enable_optimizer = 0, max_block_size = 1, max_execution_time = 1; -- { serverError 159 }
SELECT * FROM tab FORMAT Null SETTINGS max_execution_time = 1000000000;
DROP TABLE IF EXISTS tab;
