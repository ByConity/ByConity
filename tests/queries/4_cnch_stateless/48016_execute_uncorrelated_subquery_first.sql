set enable_optimizer=1;
set enable_execute_uncorrelated_subquery=1;

use test;
DROP TABLE IF EXISTS uncorrelated;
DROP TABLE IF EXISTS uncorrelated2;
CREATE TABLE uncorrelated
(
    a Int32,
    b UInt8
) ENGINE = CnchMergeTree()
ORDER BY a;

CREATE TABLE uncorrelated2
(
    a Int32,
    b UInt8
) ENGINE = CnchMergeTree()
ORDER BY a;

insert into uncorrelated values(1,2)(2,3)(3,4)(4,5)(5,6)(2,1);
insert into uncorrelated2 values(3,4)(4,5)(5,6)(2,1);

set enable_execute_uncorrelated_subquery=0;
select * from uncorrelated where uncorrelated.a < (select count() from uncorrelated2);
select * from uncorrelated where uncorrelated.a < (select 10);
select * from uncorrelated where uncorrelated.a < (select 10 + 1);

set enable_execute_uncorrelated_subquery=1;
select * from uncorrelated where uncorrelated.a < (select count() from uncorrelated2);
select * from uncorrelated where uncorrelated.a < (select 10);
select * from uncorrelated where uncorrelated.a < (select 10 + 1);

DROP TABLE IF EXISTS uncorrelated;
DROP TABLE IF EXISTS uncorrelated2;
