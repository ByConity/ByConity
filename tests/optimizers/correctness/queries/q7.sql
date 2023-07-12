select
    *
from /*+ Broadcast_Join(q6_t2) */
    q6_t1,
    q6_t2,
    q6_t3
where
    q6_t1.a=q6_t2.a and  q6_t2.a=q6_t3.a;