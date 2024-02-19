-- test limit push
select
    sum(2)
from
    q17_t3
settings enable_push_partial_agg = 1;

select /*+enable_push_partial_agg(sum)*/
    sum(2)
from
    q17_t3
settings enable_push_partial_agg = 0;

select /*+disable_push_partial_agg(sum)*/
    sum(2)
from
    q17_t3
settings enable_push_partial_agg = 1;
