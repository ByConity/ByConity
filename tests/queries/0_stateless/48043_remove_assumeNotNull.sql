use test;
drop table if exists test_48043;
drop table if exists test_48043_local;

create table test_48043_local(a String, b Nullable(String)) engine = MergeTree order by a;
create table test_48043 (a String, b Nullable(String)) engine = Distributed('test_shard_localhost', 'test', 'test_48043_local', cityHash64(a));

set enable_optimizer=1;
EXPLAIN stats=0
SELECT
    assumeNotNull(a),
    assumeNotNull(b)
FROM test_48043
WHERE assumeNotNull(a) = 'aaa' and assumeNotNull(b) = 'bbb' settings enable_simplify_assume_not_null=1;

EXPLAIN stats=0
SELECT
    assumeNotNull(a),
    assumeNotNull(b)
FROM test_48043 settings enable_simplify_assume_not_null=1, enable_simplify_predicate_in_projection=1;

EXPLAIN stats=0
SELECT *
FROM test_48043
WHERE assumeNotNull(a) = 'aaa' and assumeNotNull(b) = 'bbb' settings enable_simplify_assume_not_null=0;

drop table if exists test_48043;
drop table if exists test_48043_local;
