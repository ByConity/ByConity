set enable_push_partial_agg=1;
select
    count()
from
(
    select /*+disable_push_partial_agg*/
        sum(a)
    from q17_t3
    group by a
);

select /*+disable_push_partial_agg*/
    count()
from
(
    select
        sum(a)
    from q17_t3
    group by a
);
