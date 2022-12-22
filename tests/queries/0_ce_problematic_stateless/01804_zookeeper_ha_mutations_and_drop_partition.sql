DROP TABLE IF EXISTS ha_mut_drop_r1;
DROP TABLE IF EXISTS ha_mut_drop_r2;

CREATE TABLE ha_mut_drop_r1(d Date, x UInt32, s String) ENGINE HaMergeTree('/clickhouse/tables/' || currentDatabase() || '/ha_mut_drop', 'r1') PARTITION BY d ORDER BY x;
select sleep(1) format Null;
CREATE TABLE ha_mut_drop_r2(d Date, x UInt32, s String) ENGINE HaMergeTree('/clickhouse/tables/' || currentDatabase() || '/ha_mut_drop', 'r2') PARTITION BY d ORDER BY x;

INSERT INTO ha_mut_drop_r1 SELECT '2021-01-01', number, toString(number) from numbers(100);
INSERT INTO ha_mut_drop_r1 SELECT '2021-01-02', number, toString(number) from numbers(100);
INSERT INTO ha_mut_drop_r1 SELECT '2021-01-03', number, toString(number) from numbers(100);

-- disable part mutation temporarily
SYSTEM STOP MERGES ha_mut_drop_r1;
SYSTEM STOP MERGES ha_mut_drop_r2;

ALTER TABLE ha_mut_drop_r1 FASTDELETE s WHERE x % 2 = 0;
SELECT sleep(1) format Null;
ALTER TABLE ha_mut_drop_r1 DROP PARTITION WHERE d = '2021-01-03';

-- enable part mutation
SYSTEM START MERGES ha_mut_drop_r1;
SYSTEM START MERGES ha_mut_drop_r2;

SELECT 'sync mutation r1';
SYSTEM SYNC MUTATION ha_mut_drop_r1;
SELECT d, sum(x), sum(toUInt32(s)) from ha_mut_drop_r1 GROUP BY d ORDER BY d;
SELECT 'sync mutation r2';
SYSTEM SYNC MUTATION ha_mut_drop_r2;
SELECT d, sum(x), sum(toUInt32(s)) from ha_mut_drop_r2 GROUP BY d ORDER BY d;

SELECT 'truncate table';
TRUNCATE TABLE ha_mut_drop_r1;
SELECT 'r1', count(*) FROM ha_mut_drop_r1;

INSERT INTO ha_mut_drop_r1 SELECT '2021-01-01', number, toString(number) from numbers(100);
INSERT INTO ha_mut_drop_r1 SELECT '2021-01-02', number, toString(number) from numbers(100);
INSERT INTO ha_mut_drop_r1 SELECT '2021-01-03', number, toString(number) from numbers(100);

ALTER TABLE ha_mut_drop_r1 FASTDELETE s WHERE x = 4;
ALTER TABLE ha_mut_drop_r1 DROP PARTITION WHERE d <= '2021-01-02';


SELECT 'sync mutation r1';
SYSTEM SYNC MUTATION ha_mut_drop_r1;
SELECT d, sum(x), sum(toUInt32(s)) from ha_mut_drop_r1 GROUP BY d ORDER BY d;
SELECT 'sync mutation r2';
SYSTEM SYNC MUTATION ha_mut_drop_r2;
SELECT d, sum(x), sum(toUInt32(s)) from ha_mut_drop_r2 GROUP BY d ORDER BY d;

DROP TABLE ha_mut_drop_r1;
DROP TABLE ha_mut_drop_r2;
