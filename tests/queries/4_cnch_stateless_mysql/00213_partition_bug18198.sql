-- source include/have_partition.inc
--disable_warnings
drop table if exists t1;
--enable_warnings

create table t1 (col1 datetime)
partition by range(datediff(col1,col1))
(partition p0 values less than (10), partition p1 values less than (30));
drop table t1;
