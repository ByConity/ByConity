SELECT groupArrayOrderBy(1, 1, 2)(x, y, z), groupArrayOrderBy(1, 0, 2)(x, y, z), groupArrayOrderBy(0, 1, 2)(x, y, z),  groupArrayOrderBy(0, 0, 2)(x, y, z) FROM (SELECT number AS x, number % 3 AS y, number % 5 AS z FROM numbers(10));
SELECT groupArrayOrderBy(1, 1)(x, y), groupArrayOrderBy(0, 1)(x, y) FROM (SELECT number AS x, number % 3 AS y FROM numbers(10));
SELECT groupArrayOrderBy(1, 1)(x, y), groupArrayOrderBy(0, 1)(x, y) FROM (SELECT number AS x, number AS y FROM numbers(10));

SELECT sumOrderBy(floats) FROM (SELECT number::float / 3.14 AS floats FROM numbers(1000)); -- { serverError 42 }
SELECT sumOrderBy(0, 1)(floats, floats), sumOrderBy(1, 1)(floats, floats) FROM (SELECT number::float / 3.14 AS floats FROM numbers(1000));

select stddev_popOrderBy(1, 1)(id, id) from VALUES(
    '`id` int, `str_0` Nullable(String), `bool` boolean, `date_0` Date', 
    (1,'abc',0,'2023-06-12'),
    (2,'abd',1,'2023-06-12'),
    (3,'baa',1,'2023-06-13'),
    (1,'bbd',0,'2023-06-14'),
    (1,NULL,1,'2023-06-17')
);

DROP TABLE IF EXISTS 03033_combinator_order_by;
CREATE TABLE 03033_combinator_order_by (x Int, p Int) ENGINE = CnchMergeTree ORDER BY x PARTITION BY p;
INSERT INTO 03033_combinator_order_by(x, p) SELECT number AS x, number % 7 AS p FROM system.numbers_mt LIMIT 10000;
SELECT groupArrayOrderBy(1, 1, 1, 3)(x, x % 3, x % 5, x), groupArrayOrderBy(1, 0, 1, 3)(x, x % 3, x % 5, x), groupArrayOrderBy(0, 1, 1, 3)(x, x % 3, x % 5, x),  groupArrayOrderBy(0, 0, 1, 3)(x, x % 3, x % 5, x) FROM 03033_combinator_order_by;
DROP TABLE 03033_combinator_order_by;