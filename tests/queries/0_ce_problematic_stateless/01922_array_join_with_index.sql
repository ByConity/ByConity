DROP TABLE IF EXISTS t_array_index;

CREATE TABLE t_array_index (n Nested(key_ String, value_ String))
ENGINE = MergeTree ORDER BY n.key_;

INSERT INTO t_array_index VALUES (['a', 'b'], ['c', 'd']);

SELECT * FROM t_array_index ARRAY JOIN n WHERE n.key_ = 'a';

DROP TABLE IF EXISTS t_array_index;
