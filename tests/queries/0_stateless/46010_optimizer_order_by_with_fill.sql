set enable_optimizer=1;
DROP TABLE IF EXISTS fill;
DROP TABLE IF EXISTS fill_local;
CREATE TABLE fill_local (date Date, str String, val Int) ENGINE = MergeTree() partition by date order by str;
create table fill as fill_local engine = Distributed(test_shard_localhost, currentDatabase(), fill_local);
INSERT INTO fill VALUES (toDate('2019-05-24'), 'sd0', 13)(toDate('2019-05-10'), 'vp7', 16)(toDate('2019-05-25'), '0ei', 17)(toDate('2019-05-30'), '3kd', 18)(toDate('2019-05-15'), 'enb', 27)(toDate('2019-06-04'), '6az', 5)(toDate('2019-05-23'), '01v', 15)(toDate('2019-05-08'), 'otf', 28)(toDate('2019-05-19'), 'yfh', 20)(toDate('2019-05-07'), '2ke', 26)(toDate('2019-05-07'), 'prh', 18)(toDate('2019-05-09'), '798', 25)(toDate('2019-05-10'), 'myj', 1);

SELECT '*** table without fill to compare ***';
SELECT * FROM fill ORDER BY date, val;

-- Some useful cases

SELECT '*** date WITH FILL, val ***';
SELECT * FROM fill ORDER BY date WITH FILL, val;

SELECT '*** date WITH FILL FROM 2019-05-01 TO 2019-05-31, val WITH FILL ***';
SELECT * FROM fill ORDER BY date WITH FILL FROM toDate('2019-05-01') TO toDate('2019-05-31'), val WITH FILL;

SELECT '*** date DESC WITH FILL, val WITH FILL FROM 1 TO 6 ***';
SELECT * FROM fill ORDER BY date DESC WITH FILL, val WITH FILL FROM 1 TO 6;

-- Some weird cases

SELECT '*** date DESC WITH FILL TO 2019-05-01 STEP -2, val DESC WITH FILL FROM 10 TO -5 STEP -3 ***';
SELECT * FROM fill ORDER BY date DESC WITH FILL TO toDate('2019-05-01') STEP -2, val DESC WITH FILL FROM 10 TO -5 STEP -3;

SELECT '*** date WITH FILL TO 2019-06-23 STEP 3, val WITH FILL FROM -10 STEP 2';
SELECT * FROM fill ORDER BY date WITH FILL TO toDate('2019-06-23') STEP 3, val WITH FILL FROM -10 STEP 2;

DROP TABLE IF EXISTS fill;
DROP TABLE IF EXISTS fill_local;
CREATE TABLE fill_local (a UInt32, b Int32) ENGINE = MergeTree() partition by a order by b;
create table fill as fill_local engine = Distributed(test_shard_localhost, currentDatabase(), fill_local);
INSERT INTO fill VALUES (1, -2), (1, 3), (3, 2), (5, -1), (6, 5), (8, 0);

SELECT '*** table without fill to compare ***';
SELECT * FROM fill ORDER BY a, b;

SELECT '*** a WITH FILL, b WITH fill ***';
SELECT * FROM fill ORDER BY a WITH FILL, b WITH fill;

SELECT '*** a WITH FILL, b WITH fill TO 6 STEP 2 ***';
SELECT * FROM fill ORDER BY a WITH FILL, b WITH fill TO 6 STEP 2;


DROP TABLE IF EXISTS fill;
DROP TABLE IF EXISTS fill_local;
CREATE TABLE fill_local (number UInt32) ENGINE = MergeTree() partition by number order by number;
create table fill as fill_local engine = Distributed(test_shard_localhost, currentDatabase(), fill_local);
INSERT INTO fill VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
SELECT
    toFloat32(number % 10) AS n,
    'original' AS source
FROM fill
WHERE (number % 3) = 1
ORDER BY n ASC WITH FILL STEP 1
LIMIT 2;

DROP TABLE fill;
DROP TABLE fill_local;