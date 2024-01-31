DROP TABLE IF EXISTS t1;

CREATE TABLE t1 ( s String, f Float32, e UInt16 ) ENGINE = CnchMergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = '100G', enable_late_materialize = 1;

INSERT INTO t1 VALUES ('111', 1, 1);

SET late_materialize_aggressive_push_down = 1;

-- SELECT s FROM t1 WHERE f AND (e = 1); -- { serverError 59 }
-- SELECT s FROM t1 WHERE f; -- { serverError 59 }
-- SELECT s FROM t1 WHERE f AND (e = 1); -- { serverError 59 }
-- SELECT s FROM t1 WHERE f AND (e = 1); -- { serverError 59 }

SELECT s FROM t1 WHERE e AND (e = 1);
-- SELECT s FROM t1 WHERE e; -- { serverError 59 }
-- SELECT s FROM t1 WHERE s AND (e = 1); -- { serverError 59 }
-- SELECT s FROM t1 WHERE e AND f AND (e = 1); -- { serverError 59 }

