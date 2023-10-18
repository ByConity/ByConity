CREATE TABLE a
(
    `number` UInt64,
    `x` MATERIALIZED x
)
ENGINE = CnchMergeTree
ORDER BY number; --{ serverError 174}

CREATE TABLE foo
(
    i Int32,
    j ALIAS j + 1
)
ENGINE = CnchMergeTree() ORDER BY i; --{ serverError 174}
