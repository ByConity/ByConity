drop table if exists 10726_t1;
drop table if exists 10726_t2;
create table 10726_t1 (a Int32, b Int32) ENGINE=CnchMergeTree() ORDER BY a settings cnch_merge_max_total_rows_to_merge=1;
INSERT INTO 10726_t1 (a,b) VALUES (0,1);
select sum(a) from 10726_t1 settings enable_optimizer=1,log_queries=1,bsp_mode=1,max_bytes_to_read_leaf=1,bsp_max_retry_num=1; --{serverError 307};
system flush logs;
select ProfileEvents['QueryBspRetryCount'] FROM system.query_log where query LIKE '%select sum(a) from 10726_t1 settings enable_optimizer=1,log_queries=1,bsp_mode=1,max_bytes_to_read_leaf=1,bsp_max_retry_num=1%' AND type > 1 ORDER BY event_time DESC LIMIT 1;