SELECT * FROM (SELECT NULL AS a, 1 AS b) AS foo
RIGHT JOIN (SELECT 1024 AS b) AS bar
ON 1 = foo.b SETTINGS enable_optimizer=0; -- { serverError 403 }

SELECT * FROM (SELECT NULL AS a, 1 AS b) AS foo
RIGHT JOIN (SELECT 1024 AS b) AS bar
ON 1 = bar.b SETTINGS enable_optimizer=0; -- { serverError 403 }
