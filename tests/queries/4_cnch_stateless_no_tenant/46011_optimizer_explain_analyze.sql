DROP database if exists analyze;
create database analyze;

use analyze;
DROP TABLE IF EXISTS analyze.unique_1;
DROP TABLE IF EXISTS analyze.unique_2;

CREATE TABLE analyze.unique_1 (id UInt64, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree() ORDER BY id ;
CREATE TABLE analyze.unique_2 (id UInt64, id2 UInt32, m2 UInt64) ENGINE = CnchMergeTree() ORDER BY id ;

set enable_optimizer=1;

explain analyze stats=0,profile=0
select distinct id
from
(
    select distinct id from unique_1
    union all
    select distinct id from unique_2
);

explain analyze stats=0,profile=0
select
    t1.id
from unique_1 t1 join unique_2 t2
on t1.id = t2.id
group by t1.id;

DROP TABLE IF EXISTS analyze.unique_1;
DROP TABLE IF EXISTS analyze.unique_2;
DROP database if exists analyze;
