select /* Broadcast_Join(q6_t1) will be covered */
    *
from /*+ Broadcast_Join(q6_t1) */
    q6_t1 /*+ Repartition_Join(q6_t1) */
        join
    q6_t2
    on q6_t1.a=q6_t2.a
        join
    q6_t3
    on  q6_t2.a=q6_t3.a;