SET enable_optimizer=1;
SET cte_mode='ENFORCED';
SET enable_join_reorder=0;
SET enable_buffer_for_deadlock_cte=1;

-- { echo }
explain with c1 as (select rand(1) x) select t1.x from c1 t1 join c1 t2 on t1.x = t2.x;
explain with c1 as (select rand(1) x), c2 as (select rand(2) x) select t1.x from c1 t1 join c2 t2 on t1.x = t2.x;
explain with c1 as (select rand(1) x), c2 as (select rand(2) x) select j1.x from (select t1.x as x from c1 t1 join c2 t2 on t1.x = t2.x) j1 join c1 t3 on j1.x = t3.x;
explain with c1 as (select rand(1) x), c2 as (select rand(2) x) select j1.x from (select t1.x as x from c1 t1 join c2 t2 on t1.x = t2.x) j1 join c2 t3 on j1.x = t3.x;
explain with c1 as (select rand(1) x), c2 as (select rand(2) x) select j1.x from c1 t1 join (select t2.x as x from c1 t2 join c2 t3 on t2.x = t3.x) j1 on t1.x = j1.x;
explain with c1 as (select rand(1) x), c2 as (select rand(2) x) select j1.x from c2 t1 join (select t2.x as x from c1 t2 join c2 t3 on t2.x = t3.x) j1 on t1.x = j1.x;
explain with c1 as (select rand(1) x), c2 as (select rand(2) x) select t1.x as x from c1 t1 join c2 t2 on t1.x = t2.x union all select t3.x as x from c1 t3 join c2 t4 on t3.x = t4.x;
explain with c1 as (select rand(1) x), c2 as (select rand(2) x) select t1.x as x from c1 t1 join c2 t2 on t1.x = t2.x union all select t3.x as x from c2 t3 join c1 t4 on t3.x = t4.x;
explain with c1 as (select rand(1) x), c2 as (select rand(2) x) select j1.x from (select t1.x as x from c1 t1 join c2 t2 on t1.x = t2.x) j1 join (select t3.x as x from c2 t3 join c1 t4 on t3.x = t4.x) j2 on j1.x = j2.x;
explain with c1 as (select rand(1) x), c2 as (select rand(2) x), c3 as (select rand(3) x) select t1.x from c1 t1 join c2 t2 on t1.x = t2.x union all select t3.x from c2 t3 join c3 t4 on t3.x = t4.x;
explain with c1 as (select rand(1) x), c2 as (select rand(2) x), c3 as (select rand(3) x) select j1.x from (select t1.x from c1 t1 join c2 t2 on t1.x = t2.x) j1 join (select t3.x from c2 t3 join c3 t4 on t3.x = t4.x) j2 on j1.x = j2.x;
explain with c1 as (select rand(1) x), c2 as (select rand(2) x) select j1.x from (select t1.x from c1 t1 join c1 t2 on t1.x = t2.x) j1 join (select t3.x from c1 t3 join c2 t4 on t3.x = t4.x) j2 on j1.x = j2.x;
explain with c1 as (select rand(1) x), c2 as (select t1.x as x from c1 t1 join c1 t2 on t1.x = t2.x) select x from c2 t3;
explain with c1 as (select rand(1) x), c2 as (select t1.x as x from c1 t1 union all (select rand(2) x)) select t2.x from c2 t2 join c1 t3 on t2.x = t3.x;

-- { echoOff }
set join_use_nulls=1;
WITH
    c1 AS
    (
        SELECT 1 AS x
    ),
    c2 AS
    (
        SELECT 1 AS x
    )
SELECT *
FROM
(
    SELECT
        t5.x AS a,
        t6.x AS b
    FROM
    (
        select t1.x from c1 t1 join c2 t2 on t1.x = t2.x
    ) AS t5
    LEFT JOIN
    (
        SELECT 2 AS x
    ) AS t6 ON t5.x = t6.x
)
WHERE isNull(b);
