SET output_format_write_statistics = 0;
set enable_optimizer=0; -- json format

SELECT count() FROM (SELECT * FROM system.numbers LIMIT 1000) WHERE 1 IN (SELECT 0 WHERE 0)
FORMAT JSON;
