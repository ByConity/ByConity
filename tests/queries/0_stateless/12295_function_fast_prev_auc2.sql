USE test;
DROP TABLE IF EXISTS test_auc;

CREATE TABLE test_auc
(
    key String,
    auc AggregateFunction(fastPrevAuc2(0.5, 0., 1.), Float64, UInt8)
)
ENGINE = CnchMergeTree
ORDER BY tuple();

-- serialize to string
SELECT fastPrevAuc2State(0.5, 0., 1., 1)(tuple.1, tuple.2) AS auc
FROM
(
    SELECT arrayJoin([(0.1, 0), (0.2, 1), (0.25, 1), (0.3, 0), (0.4, 1)]) AS tuple
);
SELECT fastPrevAuc2State(0.5, 0., 1., 1, 1)(tuple.1, tuple.2) AS auc
FROM
(
    SELECT arrayJoin([(0.1, 0), (0.2, 1), (0.25, 1), (0.35, 0), (0.55, 1)]) AS tuple
);

-- serialize to binary
SELECT fastPrevAuc2State(0.5, 0., 1.)(tuple.1, tuple.2) AS auc
FROM
(
    SELECT arrayJoin([(0.1, 0), (0.2, 1), (0.25, 1), (0.3, 0), (0.4, 1)]) AS tuple
);
SELECT fastPrevAuc2State(0.5, 0., 1., 0)(tuple.1, tuple.2) AS auc
FROM
(
    SELECT arrayJoin([(0.1, 0), (0.2, 1), (0.25, 1), (0.35, 0), (0.55, 1)]) AS tuple
);

INSERT INTO test_auc FORMAT JSONEachRow {"key": "a", "auc": "'1''2''3''2''0''0'"}, {"key": "b", "auc": "'1''2''2''2''1''0'"};

-- compare wich fastAuc2
SELECT fastAuc2(0.5, 0., 1.)(tuple.1, tuple.2) AS auc
FROM
(
    SELECT arrayJoin([(0.1, 0), (0.2, 1), (0.25, 1), (0.3, 0), (0.4, 1)]) AS tuple
);
SELECT fastAuc2(0.5, 0., 1.)(tuple.1, tuple.2) AS auc
FROM
(
    SELECT arrayJoin([(0.1, 0), (0.2, 1), (0.25, 1), (0.35, 0), (0.55, 1)]) AS tuple
);

SELECT key, fastPrevAuc2Merge(0.5, 0., 1.0)(auc) FROM test_auc GROUP BY key ORDER BY key;

DROP TABLE IF EXISTS test_auc;
