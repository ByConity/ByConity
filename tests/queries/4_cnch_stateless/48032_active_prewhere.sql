drop table if EXISTS mydata;
CREATE TABLE mydata (`A` Int64, `B` Int8, `C` String)
ENGINE = CnchMergeTree
ORDER BY A;

insert into mydata SELECT     number,     0,     if(number between 1000 and 2000, 'x', toString(number)) FROM numbers(10000);

create stats mydata format Null;
set enable_optimizer=1;

EXPLAIN
SELECT *
FROM mydata
WHERE (B = 0) AND (C = 'x');

set enable_active_prewhere=1;

EXPLAIN
SELECT *
FROM mydata
WHERE (B = 0) AND (C = 'x');
