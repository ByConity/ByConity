DROP TABLE IF EXISTS t40100_imp_cast_1;
DROP TABLE IF EXISTS t40100_imp_cast_2;

CREATE TABLE t40100_imp_cast_1
(
    `string_params` Map(String, LowCardinality(Nullable(String))) CODEC(ZSTD(1))
)
engine = CnchMergeTree() order by tuple();


CREATE TABLE t40100_imp_cast_2
(
    `string_params` Map(String, String) CODEC(ZSTD(1))
)
engine = CnchMergeTree() order by tuple();

explain stats = 0
select string_params{'ff'} as c
from (
    select string_params from t40100_imp_cast_1
    union all
    select string_params from t40100_imp_cast_2
) settings enable_optimizer=1;

DROP TABLE IF EXISTS t40100_imp_cast_1;
DROP TABLE IF EXISTS t40100_imp_cast_2;
