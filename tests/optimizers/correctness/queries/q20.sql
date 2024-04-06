set enable_push_partial_agg=0;
select
    count()
from
(
    select /*+enable_push_partial_agg*/
        sum(a)
    from q17_t3
    group by a
);

select /*+enable_push_partial_agg*/
    count()
from
(
    select
        sum(a)
    from q17_t3
    group by a
);
