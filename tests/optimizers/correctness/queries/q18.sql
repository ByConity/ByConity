-- test limit push
select
    *
from
    q6_t1
        left join
    q6_t2
    on q6_t1.a=q6_t2.a
        left join
    q6_t3
    on  q6_t2.a=q6_t3.a
    where q6_t1.b = 1
    limit 10;