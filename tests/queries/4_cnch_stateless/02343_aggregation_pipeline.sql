set max_threads = 16;
set enable_optimizer=0;
-- { echoOn }

explain pipeline select * from (select * from numbers_mt(1e8) group by number) group by number;

explain pipeline select * from (select * from numbers_mt(1e8) group by number) order by number;
