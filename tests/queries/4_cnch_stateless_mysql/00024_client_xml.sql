
-- Can't run with embedded server
-- source include/not_embedded.inc

-- Disable concurrent inserts to avoid test failures when reading
-- data from concurrent connections (insert might return before
-- the data is actually in the table).
-- set @old_concurrent_insert= @@global.concurrent_insert;
-- set @@global.concurrent_insert= 0;
--disable_warnings
drop table if exists t1;
--enable_warnings

-- Test of the xml output of the 'mysql' and 'mysqldump' clients -- makes
-- sure that basic encoding issues are handled properly
create table t1 (
  `a&b` int,
  `a<b` int,
  `a>b` text
);
insert into t1 values (1, 2, 'a&b a<b a>b');

-- Determine the number of open sessions
--source include/count_sessions.inc

select * from t1;
select 1 < 2 from dual;
select 1 > 2 from dual;
select 1 & 3 from dual;
select null from dual;
select 1 limit 0;
select 1 limit 0;

drop table t1;
