SELECT fastauc2(tuple.1, tuple.2) AS auc
FROM
(
    SELECT arrayJoin([(0.1, 0), (0.2, 1), (0.3, 0), (0.4, 1)]) AS tuple
);
SELECT fastauc2(0.25, 0.0, 1.0)(tuple.1, tuple.2) AS auc
FROM
(
    SELECT arrayJoin([(0.1, 0), (0.2, 1), (0.3, 0), (0.4, 1)]) AS tuple
);
