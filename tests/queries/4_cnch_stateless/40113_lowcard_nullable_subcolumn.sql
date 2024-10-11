DROP TABLE IF EXISTS t40113;
DROP TABLE IF EXISTS t40113_2;

CREATE TABLE t40113
(
    `dsp_id` LowCardinality(Nullable(String)),
    `unit_id` LowCardinality(Nullable(String))
)
ENGINE = CnchMergeTree() order by tuple();

CREATE TABLE t40113_2
(
    `dsp_id` Nullable(String),
    `unit_id` Nullable(String)
)
ENGINE = CnchMergeTree() order by tuple();

INSERT INTO t40113 VALUES ('foo', 'bar');
INSERT INTO t40113_2 VALUES ('foo', 'bar');

SELECT dsp_id.null FROM t40113_2 FORMAT Null;
-- SELECT dsp_id.null FROM t40113 FORMAT Null; -- { serverError 44 }

SELECT 1 AS tag
FROM t40113
WHERE 1 = 1
GROUP BY 1
ORDER BY 1 ASC
FORMAT Null;

DROP TABLE IF EXISTS t40113;
DROP TABLE IF EXISTS t40113_2;
