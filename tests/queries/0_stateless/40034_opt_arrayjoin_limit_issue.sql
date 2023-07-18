SET enable_optimizer = 1;
SET enable_optimizer_white_list = 0;

SELECT x, arrayJoin(['a', 'b', 'c']) FROM (SELECT 1 as x) LIMIT 1;
EXPLAIN SELECT x, arrayJoin(['a', 'b', 'c']) FROM (SELECT 1 as x) LIMIT 1;
