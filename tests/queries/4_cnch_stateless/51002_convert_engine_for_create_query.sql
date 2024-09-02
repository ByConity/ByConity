SET enable_cnch_engine_conversion=1;
CREATE TABLE engine_conv (d Date, t Int32) Engine=MergeTree PARTITION BY d ORDER BY t;
SELECT engine FROM system.tables WHERE database = currentDatabase(1) and name = 'engine_conv';

CREATE TABLE as_engine_conv Engine=MergeTree PARTITION BY d ORDER BY t AS SELECT * FROM engine_conv; 
SELECT engine FROM system.tables WHERE database = currentDatabase(1) and name = 'as_engine_conv';

CREATE TABLE engine_aggregating_conv
(
    t DateTime,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = CnchAggregatingMergeTree()
PARTITION BY toYYYYMMDD(t)
ORDER BY t;

SELECT engine FROM system.tables WHERE database = currentDatabase(1) and name = 'engine_aggregating_conv';

SET enable_cnch_engine_conversion=0;
CREATE TABLE engine_conv_err (d Date, t Int32) Engine=MergeTree PARTITION BY d ORDER BY t; -- {serverError 79}
DROP TABLE IF EXISTS engine_conv_err;
DROP TABLE IF EXISTS engine_aggregating_conv;
DROP TABLE IF EXISTS as_engine_conv;
DROP TABLE IF EXISTS engine_conv;
