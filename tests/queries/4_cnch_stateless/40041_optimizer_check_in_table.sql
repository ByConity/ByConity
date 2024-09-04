WITH x AS (SELECT 1)
SELECT value
FROM system.settings
WHERE name = 'enable_optimizer' AND 1 IN x
SETTINGS enable_optimizer=1;
