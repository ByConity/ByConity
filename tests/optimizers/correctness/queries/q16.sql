select
    count(*)
from
    q16_t1
where
    q16_t1.a >(
        select
            count(*)
        from
            q16_t2
        )