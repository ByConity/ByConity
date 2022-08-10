SET ha_alter_metadata_sync = 2, ha_alter_data_sync = 2;

drop table if exists test.test_ttl1;
drop table if exists test.test_ttl2;

CREATE TABLE test.test_ttl1 (`datetime` DateTime, `id` Int32) ENGINE = HaMergeTree('/clickhouse/tables/test/test_ttl_case', '1') PARTITION BY datetime ORDER BY id TTL datetime + toIntervalMinute(3) SETTINGS index_granularity = 8192;
CREATE TABLE test.test_ttl2 (`datetime` DateTime, `id` Int32) ENGINE = HaMergeTree('/clickhouse/tables/test/test_ttl_case', '2') PARTITION BY datetime ORDER BY id TTL datetime + toIntervalMinute(3) SETTINGS index_granularity = 8192;
insert into table test.test_ttl1 values (now()-1, 1);
insert into table test.test_ttl1 values (now()-1, 2);
insert into table test.test_ttl1 values (now()-1, 3);
insert into table test.test_ttl1 values (now()-1, 4);

select id from test.test_ttl1 order by id;

alter table test.test_ttl1 modify ttl datetime + interval 1 second;
show create table test.test_ttl1;
show create table test.test_ttl2;

optimize table test.test_ttl1;

select id from test.test_ttl1 order by id;

alter table test.test_ttl2 modify ttl datetime + interval 3 minute;

show create table test.test_ttl1;
show create table test.test_ttl2;

insert into table test.test_ttl2 values (now()-1, 5);
insert into table test.test_ttl2 values (now()-1, 6);
insert into table test.test_ttl2 values (now()-1, 7);
insert into table test.test_ttl2 values (now()-1, 8);

optimize table test.test_ttl1;

select id from test.test_ttl2 order by id;

drop table if exists test.test_ttl2;
CREATE TABLE test.test_ttl2 (`datetime` DateTime, `id` Int32) ENGINE = HaMergeTree('/clickhouse/tables/test/test_ttl_case', '2') PARTITION BY datetime ORDER BY id TTL datetime + toIntervalMinute(3) SETTINGS index_granularity = 8192;

drop table if exists test.test_ttl1;
drop table if exists test.test_ttl2;
CREATE TABLE test.test_ttl1 (`datetime` DateTime, `id` Int32) ENGINE = HaMergeTree('/clickhouse/tables/test/test_ttl_case', '1') PARTITION BY datetime ORDER BY id  SETTINGS index_granularity = 8192;
CREATE TABLE test.test_ttl2 (`datetime` DateTime, `id` Int32) ENGINE = HaMergeTree('/clickhouse/tables/test/test_ttl_case', '2') PARTITION BY datetime ORDER BY id  SETTINGS index_granularity = 8192;

drop table if exists test.test_ttl2;
alter table test.test_ttl1 modify ttl datetime + interval 1 second;
CREATE TABLE test.test_ttl2 (`datetime` DateTime, `id` Int32) ENGINE = HaMergeTree('/clickhouse/tables/test/test_ttl_case', '2') PARTITION BY datetime ORDER BY id TTL datetime + toIntervalSecond(1) SETTINGS index_granularity = 8192;

drop table if exists test.test_ttl1;
drop table if exists test.test_ttl2;
