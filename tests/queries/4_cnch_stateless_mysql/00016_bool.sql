--
-- Test of boolean operations with NULL
--

--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings

SELECT IF(NULL AND 1, 1, 2), IF(1 AND NULL, 1, 2);
SELECT NULL AND 1, 1 AND NULL, 0 AND NULL, NULL and 0;

create table t1 (a int);
insert into t1 values (0),(1),(NULL);
SELECT * FROM t1 WHERE IF(a AND 1, 0, 1);
SELECT * FROM t1 WHERE IF(1 AND a, 0, 1);
SELECT * FROM t1 where NOT(a AND 1);
SELECT * FROM t1 where NOT(1 AND a);
SELECT * FROM t1 where (a AND 1)=0;
SELECT * FROM t1 where (1 AND a)=0;
SELECT * FROM t1 where (1 AND a)=1;
SELECT * FROM t1 where (1 AND a) IS NULL;

-- WL#638 - Behaviour of NOT does not follow SQL specification
-- set sql_mode='high_not_precedence';
select * from t1 where not a between 2 and 3;
-- set sql_mode=default;
select * from t1 where not a between 2 and 3;

-- SQL boolean tests
select a, a = false, a = true from t1;
select a, a = not false, a = not true from t1;

DROP TABLE t1;


-- Test boolean operators in select part
-- NULLs are represented as N for readability
-- Read nA as !A, AB as A && B, AoB as A || B
-- Result table makes ANSI happy

create table t1 (a int, b int);
insert into t1 values(null, null), (0, null), (1, null), (null, 0), (null, 1), (0, 0), (0, 1), (1, 0), (1, 1);

-- Below test is valid untill we have True/False implemented as 1/0
-- To comply to all rules it must show that:  n(AB) = nAonB,  n(AoB) = nAnB 

select ifnull(a, 'N') as A, ifnull(b, 'N') as B, ifnull(not a, 'N') as nA, ifnull(not b, 'N') as nB, ifnull(a and b, 'N') as AB, ifnull(not (a and b), 'N') as `n(AB)`, ifnull((not a or not b), 'N') as nAonB, ifnull(a or b, 'N') as AoB, ifnull(not(a or b), 'N') as `n(AoB)`, ifnull(not a and not b, 'N') as nAnB from t1;

-- This should work with any internal representation of True/False
-- Result must be same as above

select ifnull(a=1, 'N') as A, ifnull(b=1, 'N') as B, ifnull(not (a=1), 'N') as nA, ifnull(not (b=1), 'N') as nB, ifnull((a=1) and (b=1), 'N') as AB, ifnull(not ((a=1) and (b=1)), 'N') as `n(AB)`, ifnull((not (a=1) or not (b=1)), 'N') as nAonB, ifnull((a=1) or (b=1), 'N') as AoB, ifnull(not((a=1) or (b=1)), 'N') as `n(AoB)`, ifnull(not (a=1) and not (b=1), 'N') as nAnB from t1;

drop table t1;

-- End of 4.1 tests
