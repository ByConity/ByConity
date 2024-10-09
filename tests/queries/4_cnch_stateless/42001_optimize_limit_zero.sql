DROP TABLE IF EXISTS t1;

CREATE TABLE t1(a Int32, b Int32) ENGINE = CnchMergeTree() PARTITION BY `a` PRIMARY KEY `a` ORDER BY `a` SETTINGS index_granularity = 8192;

set enable_optimizer = 1;

explain select
    *
from
    (
        select * from t1 limit 0
        union all
        select * from t1
    );


DROP TABLE IF EXISTS t1;
