-- this issue happens in 2-nodes cluster

DROP TABLE IF EXISTS t40058;
CREATE TABLE t40058(a String, b Int32) ENGINE = CnchMergeTree() ORDER BY a;

INSERT INTO t40058 VALUES ('a', 1), ('a', 1);
INSERT INTO t40058 VALUES ('a', 2), ('b', 1);
INSERT INTO t40058 VALUES ('b', 2), ('b', 2);

SELECT a FROM t40058 GROUP BY GROUPING SETS(a) ORDER BY a settings enable_optimizer=1, group_by_two_level_threshold=1, group_by_two_level_for_grouping_set=0;

SELECT a FROM t40058 GROUP BY GROUPING SETS(a) ORDER BY a settings enable_optimizer=1, group_by_two_level_threshold=1, group_by_two_level_for_grouping_set=1;

DROP TABLE IF EXISTS t40058;
