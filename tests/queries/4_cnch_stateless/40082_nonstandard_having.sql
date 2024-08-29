set dialect_type='CLICKHOUSE';
set enable_optimizer=1;

select a, count(*) from (select 1 as a, 2 as b) group by a having b = 1;
