SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) order by x, y;
