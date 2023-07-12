select /*+ Leading(((q6_t5, q6_t3),(q6_t2, q6_t1))) */
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