DROP TABLE IF EXISTS xirr;
CREATE TABLE xirr (`id` Int64, `key1` Float64, `key2` Float32, `p_date` Date) ENGINE = CnchMergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO xirr VALUES (1,-1000,0,'2019-01-01'),(1,-1000,0,'2019-07-01'),(1,2200,0,'2019-12-31');
INSERT INTO xirr VALUES (2,0,-1186,'2020-01-16'),(2,0,-1355,'2020-02-17'),(2,0,-1453,'2020-03-16'),(2,0,-1583,'2020-04-21'),(2,0,-1496,'2020-05-15'),(2,0,6873,'2020-05-23');

SELECT xirr(key1, toUnixTimestamp(toUInt64(p_date))) FROM xirr;
SELECT xirr(0.1)(key1, toUnixTimestamp(toUInt64(p_date))) FROM xirr;
SELECT xirr(key2, toUnixTimestamp(toUInt64(p_date))) FROM xirr;
SELECT xirr(-0.1)(key2, toUnixTimestamp(toUInt64(p_date))) FROM xirr;
