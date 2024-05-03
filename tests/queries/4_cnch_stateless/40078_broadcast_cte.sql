CREATE TABLE t40078_broadcast_cte
(
    a Int32,
    b String
) ENGINE = CnchMergeTree() ORDER by a;

insert into t40078_broadcast_cte values (1,'1'), (2,'2'),(3,'3');

create stats t40078_broadcast_cte format Null;

WITH cte AS
(
    SELECT
       b,
       sum(a) total
    FROM t40078_broadcast_cte
    GROUP BY b
)
SELECT
    cte1.* 
FROM cte cte1
inner join cte cte2 on cte1.b = cte2.b
left semi join (select b, total from cte where b = '1') cte3 on cte2.total = cte3.total settings cte_mode='SHARED';
