SELECT 1 as a UNION ALL SELECT '1' as a; -- { serverError 386 }
SELECT count() FROM (SELECT 1 as a) JOIN (SELECT '1' as a) USING a; -- { serverError 53 }
SELECT count() FROM (SELECT 1 as a) t1 JOIN (SELECT '1' as a) t2 ON t1.a = t2.a; -- { serverError 53 }
SELECT count() FROM (SELECT 1 as a UNION ALL SELECT 2) t1, (SELECT '1' as a UNION ALL SELECT '2') t2 where t1.a = t2.a; -- { serverError 53 }

SET dialect_type = 'MYSQL';

SELECT 1 as a UNION ALL SELECT '1' as a FORMAT Null;
SELECT count() FROM (SELECT 1 as a) JOIN (SELECT '1' as a) USING a FORMAT Null;
SELECT count() FROM (SELECT 1 as a) t1 JOIN (SELECT '1' as a) t2 ON t1.a = t2.a FORMAT Null;
SELECT count() FROM (SELECT 1 as a UNION ALL SELECT 2) t1, (SELECT '1' as a UNION ALL SELECT '2') t2 where t1.a = t2.a;
