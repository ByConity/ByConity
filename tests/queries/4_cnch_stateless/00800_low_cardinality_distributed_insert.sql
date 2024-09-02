SET insert_distributed_sync = 1;

DROP TABLE IF EXISTS low_cardinality;

CREATE TABLE low_cardinality (d Date, x UInt32, s LowCardinality(String)) ENGINE = CnchMergeTree order by (d, x);

INSERT INTO low_cardinality (d,x,s) VALUES ('2018-11-12',1,'123');
SELECT s FROM low_cardinality;

DROP TABLE IF EXISTS low_cardinality;
