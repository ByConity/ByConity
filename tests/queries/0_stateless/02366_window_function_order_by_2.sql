SELECT row_number() over ()
FROM (select number AS a, -number AS b, number % 3 AS c FROM numbers(10))
ORDER BY a * a, b * b, - c * c;

SELECT row_number() over (), a * 2
FROM (select number AS a, -number AS b, number % 3 AS c FROM numbers(10))
WHERE c > -1
ORDER BY a + b * 0.5 - c;

SELECT row_number() over ()
FROM (select number AS a, -number AS b, number % 3 AS c FROM numbers(10))
WHERE a > 0
  AND b < -1
ORDER BY a + b * 0.5, b, c * -1.0;

SELECT row_number() over (), sum(number) as ss
FROM numbers(10)
ORDER BY ss;

SELECT row_number() over (), sum(number) as ss, max(number) as mm
FROM numbers(10)
ORDER BY ss + mm;
