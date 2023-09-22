SET enable_nested_loop_join = 1;

DROP TABLE IF EXISTS nse_lhs;
DROP TABLE IF EXISTS nse_rhs;

CREATE TABLE nse_lhs (key int, value Nullable(UInt8)) ENGINE=CnchMergeTree() order by key;
CREATE TABLE nse_rhs (key int, value Nullable(UInt8)) ENGINE=CnchMergeTree() order by key;

INSERT INTO nse_lhs VALUES (1,1) (2, 2) (3, NULL) (4, NULL) (5,6) (6, NULL);
INSERT INTO nse_rhs VALUES (1,1) (2, NULL) (3, 2) (4, NULL) (5,7) (6, 0);

SET join_algorithm='nested_loop';
SELECT key, value FROM nse_lhs JOIN nse_rhs using(key) order by key, value;

SET join_algorithm='hash';
SELECT key, value FROM nse_lhs JOIN nse_rhs using(key) order by key, value;
