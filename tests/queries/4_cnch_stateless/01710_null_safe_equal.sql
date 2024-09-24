DROP TABLE IF EXISTS nse_lhs;
DROP TABLE IF EXISTS nse_rhs;

CREATE TABLE nse_lhs (key int, value Nullable(UInt8)) ENGINE=CnchMergeTree ORDER BY tuple();
CREATE TABLE nse_rhs (key int, value Nullable(UInt8)) ENGINE=CnchMergeTree ORDER BY tuple();

INSERT INTO nse_lhs VALUES (1,1) (2, 2) (3, NULL) (4, NULL) (5,6) (6, NULL);
INSERT INTO nse_rhs VALUES (1,1) (2, NULL) (3, 2) (4, NULL) (5,7) (6, 0);

select '# Null safe join (equi hash join)';
SELECT key, value FROM nse_lhs JOIN nse_rhs ON nse_lhs.key=nse_rhs.key AND nse_rhs.value IS NOT DISTINCT FROM nse_lhs.value;
select '';

select '# Null safe join (equi hash join) non-nullable<=>non-nullable';
SELECT key, value FROM nse_lhs JOIN nse_rhs ON nse_lhs.key<=>nse_rhs.key AND nse_rhs.value <=> nse_lhs.value;
select '';

select '# Null safe join (equi hash join) non-nullable<=>nullable';
SELECT key, value FROM nse_lhs JOIN nse_rhs ON nse_lhs.key<=>nse_rhs.key AND nse_rhs.key <=> nse_lhs.value;
select '';

SET join_algorithm='partial_merge';
select '# Null safe join (merge join)';
SELECT key, value FROM nse_lhs JOIN nse_rhs ON nse_lhs.key=nse_rhs.key AND nse_rhs.value<=>nse_lhs.value settings enable_optimizer = 0;
SELECT key, value FROM nse_lhs JOIN nse_rhs ON nse_lhs.key<=>nse_rhs.key AND nse_rhs.value <=> nse_lhs.value settings enable_optimizer = 0;
SELECT key, value FROM nse_lhs JOIN nse_rhs ON nse_lhs.key<=>nse_rhs.key AND nse_rhs.key <=> nse_lhs.value settings enable_optimizer = 0;
select '';

SET join_algorithm='nested_loop';
select '# Null safe join (nested loop join)';
SELECT key, value FROM nse_lhs JOIN nse_rhs ON nse_lhs.key=nse_rhs.key AND nse_rhs.value<=>nse_lhs.value;
SELECT key, value FROM nse_lhs JOIN nse_rhs ON nse_lhs.key<=>nse_rhs.key AND nse_rhs.value <=> nse_lhs.value;
SELECT key, value FROM nse_lhs JOIN nse_rhs ON nse_lhs.key<=>nse_rhs.key AND nse_rhs.key <=> nse_lhs.value;
select '';

select '# Null safe equal';
SELECT NULL<=>NULL;
SELECT NULL<=>0;
SELECT 0 IS DISTINCT FROM NULL;
SELECT 0 IS NOT DISTINCT FROM NULL;
select '';

select '# IS DISTINCT FROM';
SELECT value IS DISTINCT FROM NULL FROM nse_rhs;
select '';

select '# USING <=> hash';
SET join_using_null_safe=1;
SET join_algorithm='hash';
SELECT key, value FROM nse_lhs JOIN nse_rhs USING (value, key);
SELECT key FROM ( SELECT 1 as key) RIGHT JOIN (SELECT 1 as key) USING key;
select '';

select '# USING <=> partial_merge';
SET join_algorithm='partial_merge';
SELECT key, value FROM nse_lhs JOIN nse_rhs USING (value, key) settings enable_optimizer = 0;
select '';

select '# USING <=> nested_loop';
SET join_algorithm='nested_loop';
SELECT key, value FROM nse_lhs JOIN nse_rhs USING (value, key);

DROP TABLE IF EXISTS nse_lhs;
DROP TABLE IF EXISTS nse_rhs;
