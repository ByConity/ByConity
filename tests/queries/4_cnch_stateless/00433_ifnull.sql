SELECT ifNull('x', 'y') AS res, toTypeName(res);
SELECT ifNull(materialize('x'), materialize('y')) AS res, toTypeName(res);

SELECT ifNull(toNullable('x'), 'y') AS res, toTypeName(res);
SELECT ifNull(toNullable('x'), materialize('y')) AS res, toTypeName(res);

SELECT ifNull('x', toNullable('y')) AS res, toTypeName(res);
SELECT ifNull(materialize('x'), toNullable('y')) AS res, toTypeName(res);

SELECT ifNull(toNullable('x'), toNullable('y')) AS res, toTypeName(res);

SELECT ifNull(toString(number), toString(-number)) AS res, toTypeName(res) FROM system.numbers LIMIT 5;
SELECT ifNull(nullIf(toString(number), '1'), toString(-number)) AS res, toTypeName(res) FROM system.numbers LIMIT 5;
SELECT ifNull(toString(number), nullIf(toString(-number), '-3')) AS res, toTypeName(res) FROM system.numbers LIMIT 5;
SELECT ifNull(nullIf(toString(number), '1'), nullIf(toString(-number), '-3')) AS res, toTypeName(res) FROM system.numbers LIMIT 5;

SELECT ifNull(NULL, 1) AS res, toTypeName(res);
SELECT ifNull(1, NULL) AS res, toTypeName(res);
SELECT ifNull(NULL, NULL) AS res, toTypeName(res);

SELECT IFNULL(NULLIF(toString(number), '1'), NULLIF(toString(-number), '-3')) AS res, toTypeName(res) FROM system.numbers LIMIT 5;

set dialect_type='MYSQL';
drop table if exists 00433_t1;
drop table if exists 00433_t2;

SELECT ifNull(toNullable('x'), toNullable(1)) AS res, toTypeName(res);
SELECT NULLIF(5,5) IS NULL, NULLIF(5,'5') IS NOT NULL;
create table 00433_t1 (a int, b int);
insert into 00433_t1 values(null, null), (0, null), (1, null), (null, 0), (null, 1), (0, 0), (0, 1), (1, 0), (1, 1);
select ifnull(a, 'N') as A, ifnull(b, 'N') as B, ifnull(not a, 'N') as nA, ifnull(not b, 'N') as nB, ifnull(a and b, 'N') as AB, ifnull(not (a and b), 'N') as `n(AB)`, ifnull((not a or not b), 'N') as nAonB, ifnull(a or b, 'N') as AoB, ifnull(not(a or b), 'N') as `n(AoB)`, ifnull(not a and not b, 'N') as nAnB from 00433_t1;

select CASE 'b' when 'a' then 1 when 'B' then 2 WHEN 'b' then 'ok' END;

create table 00433_t2 (num  double(12,2));
insert into 00433_t2 values (144.54);
select sum(if(num is null,0.00,num)) from 00433_t2;
drop table if exists 00433_t1;
drop table if exists 00433_t2;
