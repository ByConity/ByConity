drop table if exists 10725_t1;
drop table if exists 10725_t2;
create table 10725_t1 (a Int32, b Int32) ENGINE=CnchMergeTree() ORDER BY a settings cnch_merge_max_total_rows_to_merge=1;
INSERT INTO 10725_t1 (a,b) VALUES (0,1);
INSERT INTO 10725_t1 (a,b) VALUES (1,1);
INSERT INTO 10725_t1 (a,b) VALUES (0,1);
INSERT INTO 10725_t1 (a,b) VALUES (1,1);
create table 10725_t2 (a Int32, b Int32) ENGINE=CnchMergeTree() ORDER BY a settings cnch_merge_max_total_rows_to_merge=1;
INSERT INTO 10725_t2 (a,b) VALUES (0,1);
INSERT INTO 10725_t2 (a,b) VALUES (1,1);
INSERT INTO 10725_t2 (a,b) VALUES (0,1);
INSERT INTO 10725_t2 (a,b) VALUES (1,1);

create stats 10725_t1 FORMAT Null;
create stats 10725_t2 FORMAT Null;
explain SELECT a FROM
(
    WITH tbl AS
    (
    select toInt64(b + 1) AS a
    FROM
    (
    select 10725_t1.a AS a, count() AS b from 10725_t1
    JOIN (
        select a from 10725_t1
    ) t2 ON 10725_t1.a=t2.a
    GROUP BY 10725_t1.a
    )
    GROUP BY toInt64(b + 1)
    )
    select a from tbl UNION ALL
    select a from tbl UNION ALL 
    select a from 10725_t2
)
ORDER BY a settings enable_optimizer=1;

select '--------coalescing--------';

SELECT a FROM
(
    WITH tbl AS
    (
    select toInt64(b + 1) AS a
    FROM
    (
    select 10725_t1.a AS a, count() AS b from 10725_t1
    JOIN (
        select a from 10725_t1
    ) t2 ON 10725_t1.a=t2.a
    GROUP BY 10725_t1.a
    )
    GROUP BY toInt64(b + 1)
    )
    select a from tbl UNION ALL
    select a from tbl UNION ALL 
    select a from 10725_t2
)
ORDER BY a
settings enable_optimizer=1,bsp_mode=1,disk_shuffle_advisory_partition_size=100000;

select '--------no coalescing--------';

SELECT a FROM
(
    WITH tbl AS
    (
    select toInt64(b + 1) AS a
    FROM
    (
    select 10725_t1.a AS a, count() AS b from 10725_t1
    JOIN (
        select a from 10725_t1
    ) t2 ON 10725_t1.a=t2.a
    GROUP BY 10725_t1.a
    )
    GROUP BY toInt64(b + 1)
    )
    select a from tbl UNION ALL
    select a from tbl UNION ALL 
    select a from 10725_t2
)
ORDER BY a
settings enable_optimizer=1,bsp_mode=1,disk_shuffle_advisory_partition_size=1;

select '--------enfored no coalescing--------';

SELECT a FROM
(
    WITH tbl AS
    (
    select toInt64(b + 1) AS a
    FROM
    (
    select 10725_t1.a AS a, count() AS b from 10725_t1
    JOIN (
        select a from 10725_t1
    ) t2 ON 10725_t1.a=t2.a
    GROUP BY 10725_t1.a
    )
    GROUP BY toInt64(b + 1)
    )
    select a from tbl UNION ALL
    select a from tbl UNION ALL 
    select a from 10725_t2
)
ORDER BY a
settings enable_optimizer=1,bsp_mode=1,disk_shuffle_advisory_partition_size=100000,enable_disk_shuffle_partition_coalescing=0;

drop table if exists 10725_t1;
drop table if exists 10725_t2;
