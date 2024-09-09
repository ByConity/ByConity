SET dialect_type = 'CLICKHOUSE';
SET enable_optimizer = 1;
SET rewrite_unknown_left_join_identifier = 1;

SELECT t1.id FROM ( SELECT id FROM ( SELECT 1 id) AS t1 ALL FULL OUTER JOIN ( SELECT 1 id) AS t2 USING (id)) ALL FULL OUTER JOIN ( SELECT 1 id) AS t3 USING (id);
SELECT * FROM ( SELECT t1.id FROM ( SELECT id FROM ( SELECT 1 id) AS t1 ALL FULL OUTER JOIN ( SELECT 1 id) AS t2 USING (id)) ALL FULL OUTER JOIN ( SELECT 1 id) AS t3 USING (id));
