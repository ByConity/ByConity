SET enable_optimizer=1;

SELECT round(100, 0) GROUP BY 0;
SELECT 'xx' AS field, round(count(), 0) GROUP BY field;
SELECT round(rank() over (order by 0), 0);
