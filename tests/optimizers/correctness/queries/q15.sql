with tt1 as
         (
             select
                 *
             from q15_t2
             where q15_t2.a>10
    limit 0
    )
select
    *
from
    (
        (
            select * from tt1
            union all
            select * from q15_t1
        )
        union all
        select * from tt1
    )
