drop table if exists test_bitmapjoin_data_20024;

create table test_bitmapjoin_data_20024 (type String, name String, dids BitMap64, p_date Date) engine = CnchMergeTree partition by p_date order by type settings index_granularity = 128;

insert into table test_bitmapjoin_data_20024 values ('a', 'a1', [1,2,3,4,5,6,7,8,9,10], '2021-01-01');
insert into table test_bitmapjoin_data_20024 values ('b', 'b1', [11,12,13,14,15], '2021-01-01');
insert into table test_bitmapjoin_data_20024 values ('c', 'c1', [21,22,23], '2021-01-01');
insert into table test_bitmapjoin_data_20024 values ('c', 'c2', [26,27,28], '2021-01-01');
insert into table test_bitmapjoin_data_20024 values ('d', 'd1', [31,32,33,34], '2021-01-01');
insert into table test_bitmapjoin_data_20024 values ('a', 'a2', [1,2,3,4,5,6], '2021-01-02');
insert into table test_bitmapjoin_data_20024 values ('b', 'b1', [7,8,11,12,13,14], '2021-01-02');
insert into table test_bitmapjoin_data_20024 values ('c', 'c1', [9,10,15,21,22], '2021-01-02');

-- original join
select 'orgin1', type, name, bitmapCardinality(bitmapAnd(dids, b.dids)) as num
from (
    select type, name, b.name, dids, b.dids from
    (select
        1 as position,
        type,
        name,
        dids
    from test_bitmapjoin_data_20024
    where p_date = '2021-01-01') as a
    inner join
    (
        select
            2 as position,
            type,
            name,
            dids
        from test_bitmapjoin_data_20024
        where p_date = '2021-01-02'
    ) as b
    using type
)
order by type, name;

-- bitmapJoin
select 'agg1', tupleElement(tuples, 1) as type, tupleElement(tuples, 2) as name, tupleElement(tuples, 3) as num
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
            from test_bitmapjoin_data_20024
            where p_date = '2021-01-01'
            union all
            select
                2 as position,
                type,
                name,
                dids
            from test_bitmapjoin_data_20024
            where p_date = '2021-01-02'
        )
    )
)
order by type, name;


-- original join
select 'origin2', type, name, bitmapCardinality(bitmapOr(a_dids, b_dids)) as num
from (
    select type, name, bitmapColumnOr(dids) as a_dids, bitmapColumnOr(b.dids) as b_dids from
    (select
        1 as position,
        type,
        name,
        dids
    from test_bitmapjoin_data_20024
    where p_date = '2021-01-01') as a
    left join
    (
        select
            2 as position,
            type,
            name,
            dids
        from test_bitmapjoin_data_20024
        where p_date = '2021-01-02'
    ) as b
    using type
    group by type,name
)
order by type, name;

-- -- bitmapJoin
select 'agg2', tupleElement(tuples, 1) as type, tupleElement(tuples, 2) as name, tupleElement(tuples, 3) as num
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
            from test_bitmapjoin_data_20024
            where p_date = '2021-01-01'
            union all
            select
                2 as position,
                type,
                name,
                dids
            from test_bitmapjoin_data_20024
            where p_date = '2021-01-02'
        )
    )
)
order by type, name;

-- -- original join
select 'origin3', type, name, bitmapColumnCardinality(dids) as num
from (
    select type, name, bitmapColumnOr(b.dids) as dids from
    (select
        1 as position,
        type,
        name,
        arrayToBitmap(emptyArrayUInt8()) as dids
    from test_bitmapjoin_data_20024
    where p_date = '2021-01-01') as a
    inner join
    (
        select
            2 as position,
            type,
            name,
            dids
        from test_bitmapjoin_data_20024
        where p_date = '2021-01-02'
    ) as b
    using type
    group by type,name
)
group by type, name
order by type, name;

-- -- bitmapJoin
select 'agg3', tupleElement(tuples, 1) as type, tupleElement(tuples, 2) as name, tupleElement(tuples, 3) as num
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
            from test_bitmapjoin_data_20024
            where p_date = '2021-01-01'
            union all
            select
                2 as position,
                type,
                name,
                dids
            from test_bitmapjoin_data_20024
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
    from test_bitmapjoin_data_20024
    where p_date = '2021-01-01') as a
    inner join
    (
        select
            2 as position,
            type,
            name,
            dids
        from test_bitmapjoin_data_20024
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
            from test_bitmapjoin_data_20024
            where p_date = '2021-01-01'
            union all
            select
                2 as position,
                type,
                name,
                dids
            from test_bitmapjoin_data_20024
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
    from test_bitmapjoin_data_20024
    where p_date = '2021-01-01') as a
    inner join
    (
        select
            2 as position,
            type,
            name,
            dids
        from test_bitmapjoin_data_20024
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
            from test_bitmapjoin_data_20024
            where p_date = '2021-01-01'
            union all
            select
                2 as position,
                type,
                name,
                dids
            from test_bitmapjoin_data_20024
            where p_date = '2021-01-02'
        )
    )
)
order by type, name;

drop table test_bitmapjoin_data_20024;


--- fix for default parameter parsing when parameters.size()=7
-- Three parameters: return bitmap of left table, right is dismiss
SELECT arrayJoin(assumeNotNull(BitmapJoin(1, [3], ['1.4'])(position, id_map, join_key, group_key))) AS resTuples
FROM
(
    SELECT
        arrayToBitmap([1, 2, 3, 4, 5]) AS id_map,
        1 AS position,
        '1' AS join_key,
        '1' AS group_key
    UNION ALL
    SELECT
        arrayToBitmap([2, 3, 4, 5, 6]) AS id_map,
        2 AS position,
        '1' AS join_key,
        '1' AS group_key
);

--- Five parameters: return bitmapCardinality default, and thread_num=32
SELECT arrayJoin(assumeNotNull(BitmapJoin(1, [3], ['1.4'], 'AND', 'INNER')(position, id_map, join_key, group_key))) AS resTuples
FROM
(
    SELECT
        arrayToBitmap([1, 2, 3, 4, 5]) AS id_map,
        1 AS position,
        '1' AS join_key,
        '1' AS group_key
    UNION ALL
    SELECT
        arrayToBitmap([2, 3, 4, 5, 6]) AS id_map,
        2 AS position,
        '1' AS join_key,
        '1' AS group_key
);

-- Six parameters: return bitmapCardinality default, and thread_num is assigned
SELECT arrayJoin(assumeNotNull(BitmapJoin(1, [3], ['1.4'], 'AND', 'INNER', 2)(position, id_map, join_key, group_key))) AS resTuples
FROM
(
    SELECT
        arrayToBitmap([1, 2, 3, 4, 5]) AS id_map,
        1 AS position,
        '1' AS join_key,
        '1' AS group_key
    UNION ALL
    SELECT
        arrayToBitmap([2, 3, 4, 5, 6]) AS id_map,
        2 AS position,
        '1' AS join_key,
        '1' AS group_key
);

-- Full seven parameters, the 7th parameter set to 0, return bitmapCardinality
SELECT arrayJoin(assumeNotNull(BitmapJoin(1, [3], ['1.4'], 'AND', 'INNER', 2, 0)(position, id_map, join_key, group_key))) AS resTuples
FROM
(
    SELECT
        arrayToBitmap([1, 2, 3, 4, 5]) AS id_map,
        1 AS position,
        '1' AS join_key,
        '1' AS group_key
    UNION ALL
    SELECT
        arrayToBitmap([2, 3, 4, 5, 6]) AS id_map,
        2 AS position,
        '1' AS join_key,
        '1' AS group_key
);

-- the 7th parameter set to 1, return bitmap
SELECT arrayJoin(assumeNotNull(BitmapJoin(1, [3], ['1.4'], 'AND', 'INNER', 2, 1)(position, id_map, join_key, group_key))) AS resTuples
FROM
(
    SELECT
        arrayToBitmap([1, 2, 3, 4, 5]) AS id_map,
        1 AS position,
        '1' AS join_key,
        '1' AS group_key
    UNION ALL
    SELECT
        arrayToBitmap([2, 3, 4, 5, 6]) AS id_map,
        2 AS position,
        '1' AS join_key,
        '1' AS group_key
);
