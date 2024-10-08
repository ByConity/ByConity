DROP TABLE IF EXISTS prewhere_constant_folding;
create table prewhere_constant_folding (`p_date` Date) ENGINE = CnchMergeTree　PARTITION BY (p_date) order by tuple();
select p_date from prewhere_constant_folding prewhere 1=1 or p_date is not null  group by p_date;
DROP TABLE IF EXISTS prewhere_constant_folding;
