with v0 as (
    select number k1, number k2, number c from system.numbers limit 1
),
v1 as (
    select number k3 from system.numbers limit 1
),
v2 as (
    select k1, k2, k3, c from v0, v1 where v0.k2 = v1.k3
)
select k1, sum(c) from v2 group by k1 union all select k2, sum(c) from v2 group by k2;