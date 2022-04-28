drop table if exists test.test_local;
drop table if exists test.test_distributed;

create table test.test_local (type String, name String, dids BitMap64, p_date Date) engine = MergeTree partition by p_date order by type settings index_granularity = 128;
create table test.test_distributed (type String, name String, dids BitMap64, p_date Date) engine = Distributed(test_shard_localhost, 'test', 'test_local');

insert into table test.test_local values ('a', 'a1', [1,2,3,4,5,6,7,8,9,10], '2021-01-01');
insert into table test.test_local values ('b', 'b1', [11,12,13,14,15], '2021-01-01');
insert into table test.test_local values ('c', 'c1', [21,22,23], '2021-01-01');
insert into table test.test_local values ('c', 'c2', [26,27,28], '2021-01-01');
insert into table test.test_local values ('d', 'd1', [31,32,33,34], '2021-01-01');
insert into table test.test_local values ('a', 'a2', [1,2,3,4,5,6], '2021-01-02');
insert into table test.test_local values ('b', 'b1', [7,8,11,12,13,14], '2021-01-02');
insert into table test.test_local values ('c', 'c1', [9,10,15,21,22], '2021-01-02');

-- original join
select type, name, bitmapCardinality(bitmapAnd(dids, b.dids)) as num
from (
    select type, name, b.name, dids, b.dids from 
    (select
        1 as position,
        type,
        name,
        dids
    from test.test_local
    where p_date = '2021-01-01') as a
    inner join
    (
        select
            2 as position,
            type,
            name,
            dids
        from test.test_distributed
        where p_date = '2021-01-02'
    ) as b
    using type
)
order by type, name;

-- bitmapJoin
select tupleElement(tuples, 1) as type, tupleElement(tuples, 2) as name, tupleElement(tuples, 3) as num
from (
    select arrayJoin(arr)  as tuples
    from (
        select BitmapJoin(1, [3], ['3', '1.4'], 'AND', 'INNER')(position, dids, type, name) as arr
        from (
            select
                1 as position,
                type,
                name,
                dids
            from test.test_local
            where p_date = '2021-01-01'
            union all
            select
                2 as position,
                type,
                name,
                dids
            from test.test_distributed
            where p_date = '2021-01-02'
        )
    )
)
order by type, name;


-- original join
select type, name, bitmapCardinality(bitmapOr(a_dids, b_dids)) as num
from (
    select type, name, bitmapColumnOr(dids) as a_dids, bitmapColumnOr(b.dids) as b_dids from 
    (select
        1 as position,
        type,
        name,
        dids
    from test.test_local
    where p_date = '2021-01-01') as a
    left join
    (
        select
            2 as position,
            type,
            name,
            dids
        from test.test_distributed
        where p_date = '2021-01-02'
    ) as b
    using type
    group by type,name
)
order by type, name;

-- -- bitmapJoin
select tupleElement(tuples, 1) as type, tupleElement(tuples, 2) as name, tupleElement(tuples, 3) as num
from (
    select arrayJoin(arr)  as tuples
    from (
        select BitmapJoin(1, [3], [3, 4], 'OR', 'LEFT')(position, dids, type, name) as arr
        from (
            select
                1 as position,
                type,
                name,
                dids
            from test.test_local
            where p_date = '2021-01-01'
            union all
            select
                2 as position,
                type,
                name,
                dids
            from test.test_distributed
            where p_date = '2021-01-02'
        )
    )
)
order by type, name;

-- -- original join
select type, name, bitmapColumnCardinality(dids) as num
from (
    select type, name, bitmapColumnOr(b.dids) as dids from 
    (select
        1 as position,
        type,
        name,
        arrayToBitmap(emptyArrayUInt8()) as dids
    from test.test_local
    where p_date = '2021-01-01') as a
    inner join
    (
        select
            2 as position,
            type,
            name,
            dids
        from test.test_distributed
        where p_date = '2021-01-02'
    ) as b
    using type
    group by type,name
)
group by type, name
order by type, name;

-- -- bitmapJoin
select tupleElement(tuples, 1) as type, tupleElement(tuples, 2) as name, tupleElement(tuples, 3) as num
from (
    select arrayJoin(arr)  as tuples
    from (
        select BitmapJoin(1, [3], [3, 4], '', '')(position, dids, type, name) as arr
        from (
            select
                1 as position,
                type,
                name,
                arrayToBitmap(emptyArrayUInt8()) as dids
            from test.test_local
            where p_date = '2021-01-01'
            union all
            select
                2 as position,
                type,
                name,
                dids
            from test.test_distributed
            where p_date = '2021-01-02'
        )
    )
)
order by type, name;

-- original join
select type, name, bitmapAndnotCardinality(dids, b.dids) as num
from (
    select type, name, b.name, dids, b.dids from 
    (select
        1 as position,
        type,
        name,
        dids
    from test.test_local
    where p_date = '2021-01-01') as a
    inner join
    (
        select
            2 as position,
            type,
            name,
            dids
        from test.test_distributed
        where p_date = '2021-01-02'
    ) as b
    using type
)
order by type, name;

-- bitmapJoin
select tupleElement(tuples, 1) as type, tupleElement(tuples, 2) as name, tupleElement(tuples, 3) as num
from (
    select arrayJoin(arr)  as tuples
    from (
        select BitmapJoin(1, [3], ['3', '1.4'], 'ANDNOT', 'INNER')(position, dids, type, name) as arr
        from (
            select
                1 as position,
                type,
                name,
                dids
            from test.test_local
            where p_date = '2021-01-01'
            union all
            select
                2 as position,
                type,
                name,
                dids
            from test.test_distributed
            where p_date = '2021-01-02'
        )
    )
)
order by type, name;

-- original join
select type, name, bitmapAndnotCardinality(b.dids, dids) as num
from (
    select type, name, b.name, dids, b.dids from 
    (select
        1 as position,
        type,
        name,
        dids
    from test.test_local
    where p_date = '2021-01-01') as a
    inner join
    (
        select
            2 as position,
            type,
            name,
            dids
        from test.test_distributed
        where p_date = '2021-01-02'
    ) as b
    using type
)
order by type, name;

-- bitmapJoin
select tupleElement(tuples, 1) as type, tupleElement(tuples, 2) as name, tupleElement(tuples, 3) as num
from (
    select arrayJoin(arr)  as tuples
    from (
        select BitmapJoin(1, [3], ['3', '1.4'], 'ReverseANDNOT', 'INNER')(position, dids, type, name) as arr
        from (
            select
                1 as position,
                type,
                name,
                dids
            from test.test_local
            where p_date = '2021-01-01'
            union all
            select
                2 as position,
                type,
                name,
                dids
            from test.test_distributed
            where p_date = '2021-01-02'
        )
    )
)
order by type, name;

drop table test.test_local;
drop table test.test_distributed;