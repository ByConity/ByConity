DROP TABLE IF EXISTS t;

CREATE TABLE t (a Int32, b Int32) ENGINE = CnchMergeTree() ORDER BY a;
INSERT INTO t VALUES (10, 10)(11, 12);

-- { echo }

-- scalar subquery
select count() from t t1 prewhere 10 = (select b from t t2 where a = 10);
select count() from t t1 prewhere a = (select b from t t2 where a = 10);
select count() from t t1 prewhere 10 < (select b from t t2 where a = 11);

select count() from t t1 prewhere (a, b) = (select a, b from t t2 limit 0); -- { serverError 125 }
select count() from t t1 prewhere (a, b) != (select a, b from t t2 where a = 10);

-- in subquery
select count() from t t1 prewhere b in (select b from t t2);
select count() from t t1 prewhere b in (select b from t t2 limit 0);

select count() from t t1 prewhere (a, b) in (select a, b from t t2);
select count() from t t1 prewhere (a, b) in (select a, b from t t2 limit 0);

-- exists subquery
select count() from t t1 prewhere exists(select b from t t2) settings enable_optimizer = 1;
select count() from t t1 prewhere exists(select b from t t2 limit 0) settings enable_optimizer = 1;
select count() from t t1 prewhere not exists(select b from t t2 limit 0) settings enable_optimizer = 1;

select count() from t t1 prewhere exists(select a, b from t t2 limit 0) settings enable_optimizer = 1;
select count() from t t1 prewhere not exists(select a, b from t t2 limit 0) settings enable_optimizer = 1;

-- { echoOff }
-- clickhouse throw database not found instead of exists subquery not support
select count() from t t1 prewhere exists(select b from t t2) settings enable_optimizer = 0; -- { serverError 60 }
select count() from t t1 prewhere exists(select b from t t2 limit 0) settings enable_optimizer = 0; -- { serverError 60 }
select count() from t t1 prewhere not exists(select b from t t2 limit 0) settings enable_optimizer = 0; -- { serverError 60 }

select count() from t t1 prewhere exists(select a, b from t t2 limit 0) settings enable_optimizer = 0; -- { serverError 60 }
select count() from t t1 prewhere not exists(select a, b from t t2 limit 0) settings enable_optimizer = 0; -- { serverError 60 }
