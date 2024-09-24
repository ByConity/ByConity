drop table if exists any_func;
set dialect_type='MYSQL';
set enable_optimizer=1;
set only_full_group_by=0;
set allow_mysql_having_name_resolution=1;

create table any_func(a Int, b Int, c String) ENGINE = CnchMergeTree() partition by a order by a;

select a,b from any_func group by a;

SELECT a, b FROM any_func WHERE b < 10 GROUP BY a HAVING b < 10 ORDER BY b ASC ;

SELECT a, b FROM any_func WHERE b < 10 GROUP BY a HAVING any_func.b < 10 ORDER BY any_func.b ASC;

SELECT a, any_func.b FROM any_func WHERE b < 10 GROUP BY a ORDER BY b ASC;

SELECT a, b as bb FROM any_func WHERE b < 10 GROUP BY a HAVING bb < 10 ORDER BY bb ASC;

SELECT a, b as bb FROM any_func WHERE b < 10 GROUP BY a HAVING b < 10 ORDER BY b ASC;

select t1.a,t2.a from any_func t1 join any_func t2 on t1.a=t2.a  where t1.a < 10 and t2.a > 0 group by t1.a having t1.a < 10 and t2.a > 0;

select t1.a,t2.a as a from any_func t1 join any_func t2 on t1.a=t2.a where t1.a < 10 and t2.a > 0 group by t1.a having t1.a < 10 and t2.a > 0;

select t1.a,t2.a as a from any_func t1 join any_func t2 on t1.a=t2.a where t1.a < 10 and t2.a > 0 group by t1.a having t1.a < 10 and t2.a > 0;

select
    1 + 1 as x,
    a + a,
    b + a,
    a,
    b,
    count(a) + b,
    caseWithExpression(c, 'gdt', 1, 'baiduals', 1, count()) AS cnt_e_jc_url
from any_func
where a <10 and b > 9
group by a
having b < 10;

select
    a,
    a + b as b,
    b,
    b
from any_func
where a <10 and b > 9
group by a;

SELECT
    a,
    any(b) AS bb,
    bb
FROM any_func
where a <10 and b > 9
GROUP BY a;

SELECT a, t1.b FROM any_func t1 where t1.b > 10 GROUP BY a HAVING t1.b <20 and b > 10;

select count(a),sum(a)+if(b>10,1,2) from any_func;

select a, rank() over(PARTITION BY b order by b) from any_func group by a;

SELECT
    pt,
    incValue - LAG(incValue, 1, 0) OVER (ORDER BY pt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS incValue
FROM
(
    SELECT
        a as pt,
        countDistinct(b) AS incValue
    from any_func
) AS a
GROUP BY pt;

select aa,count(),ba+ba from (select a+b as aa ,b+b as ba from any_func) group by ba;

select a,count(),b+a from (select a+b as a ,b+b as b from any_func) group by b;

drop table if exists any_func;
