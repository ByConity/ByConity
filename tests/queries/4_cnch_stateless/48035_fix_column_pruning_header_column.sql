
drop database if exists test_grouping_sets_db;
create database test_grouping_sets_db;

DROP TABLE IF EXISTS test_grouping_sets_db.test_grouping_sets;
DROP TABLE IF EXISTS test_grouping_sets_db.temp;

CREATE TABLE test_grouping_sets_db.test_grouping_sets(
    a int,
    user String,
    age UInt64,
    class_id UInt64,
    subject_id UInt64,
    score Float,
    year UInt64,
    month UInt64,
    day UInt64,
    date_time date
)
ENGINE = CnchMergeTree
PARTITION BY date_time
ORDER BY a
FORMAT Null;
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (1, 'a', 7, 101, 1001, 78, 2023, 10, 26,'2023-10-26');
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (2, 'b', 9, 101, 1001, 92, 2023, 10, 27,'2023-10-27');
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (3, 'c', 13, 203, 1002, 72, 2023, 10, 28,'2023-10-28');
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (4, 'd', 8, 103, 1004, 95, 2023, 10, 28,'2023-10-28');
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (5, 'e', 14, 306, 1012, 67, 2022, 10, 28,'2023-10-27');
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (6, 'f', 19, 223, 1002, 29, 2023, 10, 26,'2023-10-26');
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (7, 'g', 19, 204, 1102, 52, 2023, 10, 27,'2023-10-28');
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (8, 'h', 19, 203, 1032, 72, 2023, 10, 28,'2023-10-27');
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (9, 'i', 19, 403, 1502, 82, 2023, 10, 26, '2023-10-26');
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (10, 'j', 17, 103, 1602, 79, 2023, 10, 27,'2023-10-26');
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (11, 'k', 19, 503, 1702, 62, 2023, 10, 28,'2023-10-27');
INSERT INTO test_grouping_sets_db.test_grouping_sets VALUES (12, 'l', 19, 233, 1202, 90, 2023, 10, 26,'2023-10-28');
-- 联合其他表的子查询
CREATE TABLE test_grouping_sets_db.temp(
    a Int,
    user String,
    date_time date
)
ENGINE = CnchMergeTree
PARTITION BY date_time
ORDER BY a;
insert into test_grouping_sets_db.temp select number, toString(number),today() from system.numbers limit 100;
--联合查询
SELECT user, age, class_id, subject_id, score, year, month, day, date_time, count(*) FROM test_grouping_sets_db.test_grouping_sets as a right join test_grouping_sets_db.temp as b on a.a=b.a group by GROUPING SETS (
    (user, age, class_id, subject_id, score, year, month, day,date_time),
    (user, year, month, day),
    (user, age, class_id, subject_id, score),
    (user, age),
    (user, class_id, subject_id)
    )
    order by user, age, class_id, subject_id, score, year, month, day, date_time, count(*) settings enable_optimizer=1,enable_optimizer_fallback=0,join_use_nulls=1;


DROP TABLE IF EXISTS test_grouping_sets_db.test_grouping_sets;
DROP TABLE IF EXISTS test_grouping_sets_db.temp;
drop database if exists test_grouping_sets_db;
