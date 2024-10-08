DROP TABLE IF EXISTS 48047_table;

create table 48047_table (a Int64) ENGINE=CnchMergeTree() order by a;
insert into 48047_table values (NULL), (0), (1), (2), (19);

set enable_optimizer=1;

explain select * from 48047_table where a; -- { serverError 59 }
explain select * from 48047_table where not(a);
explain select * from 48047_table where not(not(a));
