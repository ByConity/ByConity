set dialect_type = 'ANSI';

drop table if exists ctas_src;
drop table if exists ctas_dst;

create table ctas_src (id Int64) engine = CnchMergeTree order by id;
insert into ctas_src select number from numbers(5);

set enable_optimizer_for_create_select = 0;
select 'test CTAS in non-optimizer';
create table ctas_dst engine = CnchMergeTree order by id as select id from ctas_src;
select * from ctas_dst;
-- verify no leak in active txn list
select * from cnch(server, system.cnch_table_transactions) where table_uuid = (select uuid from system.cnch_tables where database = currentDatabase() and name = 'ctas_src');
drop table ctas_dst;

set enable_optimizer_for_create_select = 1;
select 'test CTAS in optimizer';
create table ctas_dst engine = CnchMergeTree order by id as select id from ctas_src;
select * from ctas_dst;
-- verify no leak in active txn list
select * from cnch(server, system.cnch_table_transactions) where table_uuid = (select uuid from system.cnch_tables where database = currentDatabase() and name = 'ctas_src');
drop table ctas_dst;

drop table if exists ctas_src;
