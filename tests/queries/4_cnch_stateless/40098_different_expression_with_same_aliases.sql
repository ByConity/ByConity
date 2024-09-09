SELECT 1 as a, 2 as a SETTINGS dialect_type = 'ANSI';
SELECT * FROM (SELECT 1 as a, 2 as a) SETTINGS dialect_type = 'ANSI';
SELECT 1 as a, 2 as a ORDER BY a SETTINGS dialect_type = 'ANSI'; -- { serverError 179 }

SELECT 1 as a, 2 as a SETTINGS dialect_type = 'CLICKHOUSE'; -- { serverError 179 }
SELECT count(*) FROM (SELECT 1 as a, 2 as a) SETTINGS dialect_type = 'CLICKHOUSE'; -- { serverError 179 }
SELECT count(*) FROM (SELECT 1 as a, 2 as a) WHERE a = 1 SETTINGS dialect_type = 'CLICKHOUSE'; -- { serverError 179 }
