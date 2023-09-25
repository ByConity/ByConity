select
    *
from /*+ use_grace_hash(q17_t1, q17_t3) */
    q17_t1
        join
    q17_t2
    on q17_t1.a=q17_t2.a
        join
    q17_t3
    on  q17_t2.a=q17_t3.a;

select
/*+ use_grace_hash(q17_t1, q17_t2) */
    *
from
    q17_t1,
    q17_t2,
    q17_t3
where  q17_t2.a=q17_t3.a and q17_t1.a=q17_t2.a
