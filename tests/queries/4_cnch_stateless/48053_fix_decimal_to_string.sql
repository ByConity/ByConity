DROP TABLE IF EXISTS 48053_table;
DROP TABLE IF EXISTS 48053_table_local;

create table 48053_table(a UInt64) ENGINE=CnchMergeTree() order by a;
insert into 48053_table values (1)(2)(3);

set dialect_type='ANSI';
select quantile(0.5)(a) from 48053_table settings enable_push_partial_agg=1;
set dialect_type='CLICKHOUSE';
select quantile(0.5)(a) from 48053_table settings enable_push_partial_agg=1;
