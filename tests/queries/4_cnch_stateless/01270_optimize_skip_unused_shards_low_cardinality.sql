set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=2;
set allow_suspicious_low_cardinality_types=1;

drop table if exists dist_01270;

create table dist_01270 (key LowCardinality(Int)) Engine = CnchMergeTree CLUSTER BY key INTO 2 BUCKETS order by tuple();
select * from dist_01270 where key = 1;

drop table dist_01270;
