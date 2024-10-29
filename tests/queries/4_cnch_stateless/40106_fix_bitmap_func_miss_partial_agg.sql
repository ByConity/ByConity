drop table if exists t40106;

create table t40106(bm BitMap64) engine = CnchMergeTree() order by tuple();

explain stats=0 select bitmapColumnCardinality(bm) from t40106 settings enable_optimizer=1;
explain stats=0 select bitmapCount('1')(toInt64(1), bm) from t40106 settings enable_optimizer=1;

drop table if exists t40106;
