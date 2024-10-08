SET enable_optimizer = 1;

DROP TABLE IF EXISTS unwrap_cast_expr_core;

CREATE TABLE unwrap_cast_expr_core(a Int32)
ENGINE = CnchMergeTree() ORDER BY a;


EXPLAIN SELECT * FROM unwrap_cast_expr_core WHERE CAST(a, 'Nullable(Int32)') > 1;

DROP TABLE IF EXISTS unwrap_cast_expr_core;
