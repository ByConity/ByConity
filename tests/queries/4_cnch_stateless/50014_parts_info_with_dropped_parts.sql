SET disable_optimize_final = 0;
select 'Alter table should not cover the base part.';

CREATE TABLE test (a int, b int, c int, d int) ENGINE = CnchMergeTree() ORDER BY d;
system start merges test;

alter table test modify setting old_parts_lifetime=10000; -- phase-one gc

insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;

ALTER TABLE test drop column a;
ALTER TABLE test drop column b;

OPTIMIZE TABLE test FINAL;

SELECT sleepEachRow(3) FROM system.numbers LIMIT 20 FORMAT Null;

select equals(
  (
    select count(), sum(bytes) from system.cnch_parts where database = currentDatabase() and table = 'test' and part_type <= 2
  ),
  (
    select total_parts_number, total_parts_size from system.cnch_parts_info where database = currentDatabase() and table = 'test'
  )
);

select equals(
  (
    select sum(rows) from system.cnch_parts where database = currentDatabase() and table = 'test' and part_type = 1
  ),
  (
    select total_rows_count from system.cnch_parts_info where database = currentDatabase() and table = 'test'
  )
);

select 'Rows count should be accurate after recalculating metrics.';
SYSTEM RECALCULATE METRICS FOR test;
SELECT sleepEachRow(3) FROM system.numbers LIMIT 6 FORMAT Null;

select equals(
  (
    select count() from test 
  ),
  (
    select total_rows_count from system.cnch_parts_info where database = currentDatabase() and table = 'test'
  )
);

DROP TABLE test;

select 'Alter table should not cover the base part.';

CREATE TABLE test (a int, b int, c int, d int) ENGINE = CnchMergeTree() ORDER BY d UNIQUE KEY d % 400;
system start merges test;

alter table test modify setting old_parts_lifetime=10000; -- phase-one gc

insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 100;

ALTER TABLE test drop column a;
ALTER TABLE test drop column b;

OPTIMIZE TABLE test FINAL;

SELECT sleepEachRow(3) FROM system.numbers LIMIT 20 FORMAT Null;

select equals(
  (
    select count(), sum(bytes) from system.cnch_parts where database = currentDatabase() and table = 'test' and part_type <= 2
  ),
  (
    select total_parts_number, total_parts_size from system.cnch_parts_info where database = currentDatabase() and table = 'test'
  )
);

select equals(
  (
    select sum(rows) from system.cnch_parts where database = currentDatabase() and table = 'test' and part_type = 1
  ),
  (
    select total_rows_count from system.cnch_parts_info where database = currentDatabase() and table = 'test'
  )
);

select equals(
  (
    select count(), sum(bytes) from system.cnch_parts where database = currentDatabase() and table = 'test' and part_type >= 3
  ),
  (
    select dropped_parts_number, dropped_parts_size from system.cnch_parts_info where database = currentDatabase() and table = 'test'
  )
);

select 'Rows count should be accurate after recalculating metrics.';
SYSTEM RECALCULATE METRICS FOR test;
SELECT sleepEachRow(3) FROM system.numbers LIMIT 6 FORMAT Null;

select equals(
  (
    select count() from test 
  ),
  (
    select total_rows_count from system.cnch_parts_info where database = currentDatabase() and table = 'test'
  )
);

DROP TABLE test;

select 'Truncate table and dropped part.';

CREATE TABLE test (a int, b int, c int, d int) ENGINE = CnchMergeTree() ORDER BY d;
system stop merges test;

insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;

TRUNCATE TABLE test;

select equals(
  (
    select count(), sum(bytes) from system.cnch_parts where database = currentDatabase() and table = 'test' and part_type >= 3
  ),
  (
    select dropped_parts_number, dropped_parts_size from system.cnch_parts_info where database = currentDatabase() and table = 'test'
  )
);

insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;
insert into test SELECT * FROM generateRandom('a int, b int, c int, d int') LIMIT 10;

TRUNCATE TABLE test;

select equals(
  (
    select count(), sum(bytes) from system.cnch_parts where database = currentDatabase() and table = 'test' and part_type >= 3
  ),
  (
    select dropped_parts_number, dropped_parts_size from system.cnch_parts_info where database = currentDatabase() and table = 'test'
  )
);

system recalculate metrics for test;
SELECT sleepEachRow(3) FROM system.numbers LIMIT 6 FORMAT Null;

select total_parts_number, total_parts_size, total_rows_count from system.cnch_parts_info where database = currentDatabase() and table = 'test';

select equals(
  (
    select count(), sum(bytes) from system.cnch_parts where database = currentDatabase() and table = 'test'
  ),
  (
    select dropped_parts_number, dropped_parts_size from system.cnch_parts_info where database = currentDatabase() and table = 'test'
  )
);
