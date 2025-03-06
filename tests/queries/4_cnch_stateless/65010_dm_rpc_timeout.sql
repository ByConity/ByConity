drop table if exists dm_rpc;

create table dm_rpc(x Int, y String) engine=CnchMergeTree() order by x;

select sleepEachRow(1);

system start gc dm_rpc;

set dm_rpc_timeout_ms=1;
system stop gc dm_rpc; -- { serverError 2005 }

set dm_rpc_timeout_ms=10000;

system stop gc dm_rpc; 
system start gc dm_rpc;

drop table dm_rpc;
