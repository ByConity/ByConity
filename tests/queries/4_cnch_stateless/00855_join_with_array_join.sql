
SET joined_subquery_requires_alias = 0;

SELECT ax, c FROM (SELECT [1,2] ax, 0 c) ARRAY JOIN ax JOIN (SELECT 0 c) USING (c);
SELECT ax, c FROM (SELECT [3,4] ax, 0 c) JOIN (SELECT 0 c) USING (c) ARRAY JOIN ax;
SELECT ax, c FROM (SELECT [5,6] ax, 0 c) s1 JOIN system.one s2 ON s1.c = s2.dummy ARRAY JOIN ax;


SELECT ax, c FROM (SELECT [101,102] ax, 0 c) s1
JOIN system.one s2 ON s1.c = s2.dummy
JOIN system.one s3 ON s1.c = s3.dummy
ARRAY JOIN ax; -- { serverError 48 }

SELECT '-';

SET joined_subquery_requires_alias = 1;


DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS d;
CREATE TABLE f (`d_ids` Array(Int64) ) ENGINE = CnchMergeTree ORDER BY d_ids;
INSERT INTO f VALUES ([1, 2]);

CREATE TABLE d (`id` Int64, `name` String ) ENGINE = CnchMergeTree ORDER BY id;

INSERT INTO d VALUES (2, 'a2'), (3, 'a3');

SELECT DISTINCT d_ids, id, name FROM f LEFT ARRAY JOIN d_ids LEFT JOIN d ON d.id = d_ids ORDER BY d_ids;
SELECT DISTINCT did, id, name FROM f LEFT ARRAY JOIN d_ids as did LEFT JOIN d ON d.id = did ORDER BY did;

-- optimizer won't reject id=id
set enable_optimizer=0;
set dialect_type='CLICKHOUSE';
-- name clash, doesn't work yet
SELECT id, name FROM f LEFT ARRAY JOIN d_ids as id LEFT JOIN d ON d.id = id ORDER BY id; -- { serverError 403 }

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS d;

