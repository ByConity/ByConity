SELECT "numbers"."number" FROM "system"."numbers" LIMIT 1;
SET dialect_type='MYSQL';
SELECT toString(number) as t FROM numbers(11) where t="10";
