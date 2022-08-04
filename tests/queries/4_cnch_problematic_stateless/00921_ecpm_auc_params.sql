SELECT ecpmauc(0.34, -1.0, 0.0)(tuple.1, tuple.2) AS auc
FROM
(
    SELECT arrayJoin([(0.1, 0), (0.2, 0.2), (0.3, 0), (0.4, 0.8)]) AS tuple
) AS t
