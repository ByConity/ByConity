select
    *
from
    q6_t1 /*+ Broadcast_Join(q6_t1) */
        join
    q6_t2
    on q6_t1.a=q6_t2.a
        join
    q6_t3
    on  q6_t2.a=q6_t3.a;