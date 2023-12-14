select
    *
from /*+ Leading(((q6_t5, q6_t3), (q6_t2, q6_t1))), Broadcast_Join(q6_t1) */
    q6_t1,
    q6_t2,
    q6_t3,
    q6_t4,
    q6_t5
where
        q6_t1.a = q6_t2.a
  and q6_t2.a = q6_t3.a
  and q6_t3.a = q6_t4.a
  and q6_t4.a = q6_t5.a;

select 
    *
from
(
select /*+ Leading(((q6_t5, q6_t3), (q6_t2, q6_t1))), Broadcast_Join(q6_t1) */
    *
from 
    q6_t1,
    q6_t2,
    q6_t3,
    q6_t4,
    q6_t5
where
        q6_t1.a = q6_t2.a
  and q6_t2.a = q6_t3.a
  and q6_t3.a = q6_t4.a
  and q6_t4.a = q6_t5.a
);

SELECT  /*+ leading(((b, a1), a2))*/ count()
FROM web AS a1, web AS a2, cust AS b
WHERE (a1.price < 10) AND (a2.price = 22) AND (a1.sk = b.sk);

select
    *
from /*+ swap_join_order(t2,t4)*/
(
    select
        t1.b as c
    from q6_t1 t1, q6_t2 t2
    where t1.b=t2.c
) as t5
left join
(
    select
        count() as c
    from
        q6_t3 t3, q6_t4 t4
    where t3.d=t4.e
) as t6
on t5.c = t6.c;