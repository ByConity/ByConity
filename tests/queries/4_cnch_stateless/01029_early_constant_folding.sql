set enable_optimizer = 0;
-- constant folding

EXPLAIN SYNTAX SELECT 1 WHERE 1 = 0;

EXPLAIN SYNTAX SELECT 1 WHERE 1 IN (0, 1, 2);

EXPLAIN SYNTAX SELECT 1 WHERE 1 IN (0, 2) AND 2 = ((SELECT 2) AS subquery);

-- no constant folding

EXPLAIN SYNTAX SELECT 1 WHERE 1 IN ((SELECT arrayJoin([1, 2, 3])) AS subquery);

EXPLAIN SYNTAX SELECT 1 WHERE NOT ignore();
