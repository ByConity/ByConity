
-- this test mostly test privilege control (what doesn't work
-- in the embedded server by default). So skip the test in embedded-server mode.
-- source include/not_embedded.inc

--Don't run this test when thread_pool active
--source include/not_threadpool.inc

-- source include/testdb_only.inc

-- set @orig_sql_mode= @@sql_mode;
-- set sql_mode= (select replace(@@sql_mode,'NO_AUTO_CREATE_USER',''));

--disable_warnings
drop table if exists t1,t2;
drop view if exists v1,v2;
drop function if exists f1;
drop function if exists f2;
--enable_warnings

use INFORMATION_SCHEMA;
--replace_result Tables_in_INFORMATION_SCHEMA Tables_in_information_schema
show tables where Tables_in_INFORMATION_SCHEMA NOT LIKE 'Innodb%' and Tables_in_INFORMATION_SCHEMA NOT LIKE 'ndb%';
--replace_result 'Tables_in_INFORMATION_SCHEMA (T%)' 'Tables_in_information_schema (T%)'
show tables from INFORMATION_SCHEMA like 'T%';
create database `inf%`;
create database mbase;
use `inf%`;
show tables;
