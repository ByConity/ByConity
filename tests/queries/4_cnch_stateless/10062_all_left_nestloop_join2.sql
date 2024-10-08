drop table IF EXISTS t7;
drop table IF EXISTS t8;

CREATE TABLE t7 (x UInt32, y UInt64) engine = CnchMergeTree() ORDER BY (x,y);
CREATE TABLE t8 (x UInt32, y UInt64) engine = CnchMergeTree() ORDER BY (x,y);

INSERT INTO t7 (x, y) VALUES (0, 0);
INSERT INTO t8 (x, y) VALUES (0, 0);

INSERT INTO t7 (x, y) VALUES (2, 20);
INSERT INTO t8 (x, y) VALUES (2, 21);
select t7.*, t8.* from t7 left join t8 on t7.x = t8.x order by x settings join_algorithm='nested_loop';
