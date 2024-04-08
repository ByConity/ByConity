drop database if exists test_12300_query_exchange_log;
create database test_12300_query_exchange_log;
drop table if exists test_12300_query_exchange_log.test_12300_query_exchange_log_t1;
create table test_12300_query_exchange_log.test_12300_query_exchange_log_t1(a Int, b Int) ENGINE = CnchMergeTree() ORDER by a;
insert into test_12300_query_exchange_log.test_12300_query_exchange_log_t1 (a,b) values (1, 2);
select * from test_12300_query_exchange_log.test_12300_query_exchange_log_t1 settings enable_optimizer=1,log_queries=1,log_query_exchange=1;
system flush logs;
select sum(recv_rows) from system.query_exchange_log where initial_query_id=(select query_id from system.query_log where query LIKE '%select * from test_12300%' AND query NOT LIKE '%system%' LIMIT 1) AND type LIKE 'brpc_receiver%';
