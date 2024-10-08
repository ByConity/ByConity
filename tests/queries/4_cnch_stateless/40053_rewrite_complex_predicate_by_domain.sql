DROP TABLE IF EXISTS lineitem40053;
DROP TABLE IF EXISTS part40053;

CREATE TABLE lineitem40053
(
    `l_orderkey` Int32,
    `l_partkey` Nullable(Int32),
    `l_suppkey` Nullable(Int32),
    `l_linenumber` Nullable(Int32),
    `l_quantity` Nullable(Float64),
    `l_extendedprice` Nullable(Float64),
    `l_discount` Nullable(Float64),
    `l_tax` Nullable(Float64),
    `l_returnflag` Nullable(String),
    `l_linestatus` Nullable(String),
    `l_shipdate` Nullable(Date),
    `l_commitdate` Nullable(Date),
    `l_receiptdate` Nullable(Date),
    `l_shipinstruct` Nullable(String),
    `l_shipmode` Nullable(String),
    `l_comment` Nullable(String)
)
ENGINE = CnchMergeTree() ORDER BY l_orderkey;

CREATE TABLE part40053
(
    `p_partkey` Int32,
    `p_name` Nullable(String),
    `p_mfgr` Nullable(String),
    `p_brand` Nullable(String),
    `p_type` Nullable(String),
    `p_size` Nullable(Int32),
    `p_container` Nullable(String),
    `p_retailprice` Nullable(Float64),
    `p_comment` Nullable(String)
)
ENGINE = CnchMergeTree() ORDER BY p_partkey;

SET dialect_type='ANSI';
SET enable_optimizer=1;
SET rewrite_complex_predicate_by_domain = 1;

EXPLAIN SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem40053 AS lineitem,
  part40053 AS part
WHERE (
    p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= 1
    AND l_quantity <= 1 + 10
    AND p_size BETWEEN 1 AND 5
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )
  OR (
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND l_quantity >= 10
    AND l_quantity <= 10 + 10
    AND p_size BETWEEN 1 AND 10
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )
  OR (
    p_partkey = l_partkey
    AND p_brand = 'Brand#34'
    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND l_quantity >= 20
    AND l_quantity <= 20 + 10
    AND p_size BETWEEN 1 AND 15
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  );

DROP TABLE IF EXISTS lineitem40053;
DROP TABLE IF EXISTS part40053;
