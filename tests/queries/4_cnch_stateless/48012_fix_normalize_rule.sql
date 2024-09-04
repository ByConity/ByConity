CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS t1;

CREATE TABLE t1(s String, a Int32, b Int32, c Int32) ENGINE = CnchMergeTree() PARTITION BY `a` PRIMARY KEY `a` ORDER BY `a` SETTINGS index_granularity = 8192;

select count() from t1 where ((s = 'a') or (a = 1) or (b = 2)) and ((s = 'a') or (c = 4)) group by a;

DROP TABLE IF EXISTS t1;
