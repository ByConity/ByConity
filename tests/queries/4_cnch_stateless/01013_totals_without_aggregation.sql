set enable_optimizer=0; -- ROLLUP/CUBE result diff
SELECT 11 AS n GROUP BY n WITH TOTALS;
SELECT 12 AS n GROUP BY n WITH ROLLUP;
SELECT 13 AS n GROUP BY n WITH CUBE;
SELECT 1 AS n WITH TOTALS; -- { serverError 48 }
SELECT 1 AS n WITH ROLLUP; -- { serverError 48 }
SELECT 1 AS n WITH CUBE; -- { serverError 48 }
