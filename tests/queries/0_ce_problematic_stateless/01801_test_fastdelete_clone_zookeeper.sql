drop table if exists test_mutation_1 sync;
drop table if exists test_mutation_2 sync;

CREATE TABLE test_mutation_1 (`p_date` Date, `id` Int32, `age` Int32) ENGINE = HaMergeTree('/clickhouse/tables/' || currentDatabase() || '/test_mutation', '1') PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192;
CREATE TABLE test_mutation_2 (`p_date` Date, `id` Int32, `age` Int32) ENGINE = HaMergeTree('/clickhouse/tables/' || currentDatabase() || '/test_mutation', '2') PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192;

SET mutations_sync = 2;
SET mutations_wait_timeout = 30;

insert into test_mutation_1 select '2021-01-01', number, 18 from system.numbers limit 81920;

optimize table test_mutation_1;

alter table test_mutation_1 fastdelete age where id >= 0 and id < 10000;
select count() from test_mutation_1;

system mark lost test_mutation_2;

alter table test_mutation_1 fastdelete age where id >= 20000 and id < 50000;

select count() from test_mutation_1;

system restart replica test_mutation_2;

alter table test_mutation_1 fastdelete age where id in (select toInt32(number * 20) from system.numbers limit 1000);

select count() from test_mutation_1;
select count() from test_mutation_2;

drop table test_mutation_1 sync;
drop table test_mutation_2 sync;
