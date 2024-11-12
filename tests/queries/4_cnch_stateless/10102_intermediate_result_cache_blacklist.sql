set enable_optimizer=1;
set wait_intermediate_result_cache=0;
set enable_optimizer_fallback=0;
set enable_intermediate_result_cache=1;

DROP TABLE if exists blacklist;

CREATE TABLE blacklist(c1 UInt64, c2 String) ENGINE = CnchMergeTree ORDER BY c1;

insert into blacklist values (1, 'a'), (2, 'b'), (2, 'c');

explain stats=0 select sum(c1) from blacklist sample 0.9 group by c2;
explain stats=0 select sum(c1) from blacklist where rand() % 2 = 1 group by c2;
explain stats=0 select sum(c1) from blacklist prewhere rand() % 2 = 1 group by c2;
explain stats=0 select sum(c1) from blacklist group by grouping sets((c2), ());
explain stats=0 select sum(c1) from (select * from blacklist order by c1 limit 1) group by c2 settings optimize_read_in_order=1, enable_sorting_property=1;
explain stats=0 select sum(c1) from (select * from blacklist limit 1) group by c2;

DROP TABLE blacklist;
