DROP TABLE IF EXISTS ha_mut_merge_r1;
DROP TABLE IF EXISTS ha_mut_merge_r2;

CREATE TABLE ha_mut_merge_r1(d Date, x UInt32, s String) ENGINE HaMergeTree('/clickhouse/tables/' || currentDatabase() || '/ha_mut_merge', 'r1') PARTITION BY d ORDER BY x;
select sleep(1) format Null;
CREATE TABLE ha_mut_merge_r2(d Date, x UInt32, s String) ENGINE HaMergeTree('/clickhouse/tables/' || currentDatabase() || '/ha_mut_merge', 'r2') PARTITION BY d ORDER BY x;

INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(10000);
INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(10000);
INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(10000);

ALTER TABLE ha_mut_merge_r1 FASTDELETE s WHERE x < 1000;

INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(1000);
INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(1000);
INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(1000);

ALTER TABLE ha_mut_merge_r1 FASTDELETE s WHERE x < 100;
OPTIMIZE TABLE ha_mut_merge_r1;

INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(100);
INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(100);
INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(100);

ALTER TABLE ha_mut_merge_r1 FASTDELETE s WHERE x < 10;

INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(10);
INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(10);
INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(10);

ALTER TABLE ha_mut_merge_r1 FASTDELETE s WHERE x < 1;

INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(1);
INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(1);
INSERT INTO ha_mut_merge_r1 SELECT '2021-01-01', number, toString(number) FROM numbers(1);

optimize table ha_mut_merge_r1;

select 'sync mutation r1';
SYSTEM SYNC MUTATION ha_mut_merge_r1; -- wait for mutations to finish
select sum(x), sum(toUInt32(s)) from ha_mut_merge_r1;

select 'sync replica r1';
SYSTEM SYNC REPLICA ha_mut_merge_r1; -- wait for merges to finish
select sum(x), sum(toUInt32(s)) from ha_mut_merge_r1;

select 'sync mutation r2';
SYSTEM SYNC MUTATION ha_mut_merge_r2; -- wait for mutations to finish
select sum(x), sum(toUInt32(s)) from ha_mut_merge_r2;

select 'sync replica r2';
SYSTEM SYNC REPLICA ha_mut_merge_r2; -- wait for merges to finish
select sum(x), sum(toUInt32(s)) from ha_mut_merge_r2;

select 'final merge';
optimize table ha_mut_merge_r1;
SYSTEM SYNC REPLICA ha_mut_merge_r1; -- wait for merges to finish
select sum(x), sum(toUInt32(s)) from ha_mut_merge_r1;

DROP TABLE IF EXISTS ha_mut_merge_r1;
DROP TABLE IF EXISTS ha_mut_merge_r2;
