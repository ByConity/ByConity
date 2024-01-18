SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2 on 1 = 1 order by x settings enable_join_on_1_equals_1=1;
SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2 on js1.x = js2.y and 1 = 1 order by x settings enable_join_on_1_equals_1=1;

