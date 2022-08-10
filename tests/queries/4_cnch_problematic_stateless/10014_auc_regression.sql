SELECT auc(1)(tuple.1, tuple.2) AS auc
FROM
(
    SELECT arrayJoin([(0.1, 0.0), (0.2, 1.5), (0.3, -1.0), (0.4, 100)]) AS tuple
) AS t