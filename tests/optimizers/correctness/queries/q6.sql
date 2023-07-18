select /*+ Broadcast_Join(q6_t2),Broadcast_Join(q6_t4) */
    *
from
    q6_t1,
    q6_t2,
    q6_t3,
    q6_t4
where
    q6_t1.a=q6_t2.a
  and  q6_t2.a=q6_t3.a
  and q6_t3.a=q6_t4.a;