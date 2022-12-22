SET dialect_type='ANSI';

DROP TABLE IF EXISTS cube;
CREATE TABLE cube(a String, b Int32, s Int32) ENGINE = Memory;

INSERT INTO cube VALUES ('a', 1, 10), ('a', 1, 15), ('a', 2, 20);
INSERT INTO cube VALUES ('a', 2, 25), ('b', 1, 10), ('b', 1, 5);
INSERT INTO cube VALUES ('b', 2, 20), ('b', 2, 15);

CREATE TABLE cube_dist(a String, b Int32, s Int32) ENGINE = Distributed(test_shard_localhost, currentDatabase(), cube);

SELECT a, b, sum(s), count() from cube_dist GROUP BY CUBE(a, b) ORDER BY a, b;

SELECT a, b, sum(s), count() from cube_dist GROUP BY CUBE(a, b) WITH TOTALS ORDER BY a, b;

SELECT a, b, sum(s), count() from cube_dist GROUP BY a, b WITH CUBE ORDER BY a, b;

SELECT a, b, sum(s), count() from cube_dist GROUP BY a, b WITH CUBE WITH TOTALS ORDER BY a, b;

SET group_by_two_level_threshold = 1;
SELECT a, b, sum(s), count() from cube_dist GROUP BY a, b WITH CUBE ORDER BY a, b;

DROP TABLE cube_dist;
DROP TABLE cube;
