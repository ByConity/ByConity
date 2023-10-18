DROP TABLE IF EXISTS test_tea_limit_local;

CREATE TABLE test_tea_limit_local(`id` Int32, `m` Int32)
ENGINE = CnchMergeTree PARTITION BY id
ORDER BY id;
    
insert into test_tea_limit_local values (1, 2);
insert into test_tea_limit_local values (1, 3);
insert into test_tea_limit_local values (2, 4);
insert into test_tea_limit_local values (2, 5);

SELECT
    id,
    sum(m) AS col_22
FROM
    test_tea_limit_local
GROUP BY id TEALIMIT 1 GROUP assumeNotNull(id) ORDER col_22 DESC;

DROP TABLE IF EXISTS test_tea_limit_local;
