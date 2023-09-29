DROP DATABASE IF EXISTS test_workload_dump;
CREATE DATABASE IF NOT EXISTS test_workload_dump;
DROP TABLE IF EXISTS test_workload_dump.test_tb;

use test_workload_dump;
CREATE TABLE test_workload_dump.test_tb(a Int32 not null, b Int32 not null) ENGINE = CnchMergeTree() PARTITION BY `a` PRIMARY KEY `a` ORDER BY `a`;
insert into test_workload_dump.test_tb values(1,2)(2,3)(3,4);
create stats all format Null;

dump ddl from test_workload_dump into '/tmp/test_dump_ddl.zip';
dump query (select a, count(b) from test_workload_dump.test_tb group by a) into '/tmp/test_dump_query.zip';

drop database test_workload_dump;
use default;

reproduce ddl source '/tmp/test_dump_ddl.zip';
drop database if exists test_workload_dump;

reproduce ddl source '/tmp/test_dump_query.zip';
reproduce source '/tmp/test_dump_query.zip' format Null;



