SET enable_optimizer = 1;

DROP TABLE IF EXISTS unwrap_cast_expr_core_local;
DROP TABLE IF EXISTS unwrap_cast_expr_core;

CREATE TABLE unwrap_cast_expr_core_local(a Int32)
ENGINE = MergeTree() ORDER BY a;

CREATE TABLE unwrap_cast_expr_core AS unwrap_cast_expr_core_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'unwrap_cast_expr_core_local');

EXPLAIN SELECT * FROM unwrap_cast_expr_core WHERE CAST(a, 'Nullable(Int32)') > 1;

DROP TABLE IF EXISTS unwrap_cast_expr_core_local;
DROP TABLE IF EXISTS unwrap_cast_expr_core;
