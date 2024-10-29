create table nation (
        n_nationkey  Int32 NOT NULL,
        n_name       LowCardinality(String NOT NULL),
        n_regionkey  Int32 NOT NULL,
        n_comment    String NOT NULL
) ENGINE = CnchMergeTree()
ORDER BY n_nationkey
CLUSTER BY n_nationkey INTO 3 BUCKETS;

create table region (
        r_regionkey  Int32 NOT NULL,
        r_name       LowCardinality(String NOT NULL),
        r_comment    LowCardinality(Nullable(String))
) ENGINE = CnchMergeTree()
ORDER BY r_regionkey
CLUSTER BY r_regionkey INTO 3 BUCKETS;

create table part (
        p_partkey     Int32 NOT NULL,
        p_name        String NOT NULL,
        p_mfgr        LowCardinality(String NOT NULL),
        p_brand       LowCardinality(String NOT NULL),
        p_type        LowCardinality(String NOT NULL),
        p_size        Int32 NOT NULL,
        p_container   LowCardinality(String NOT NULL),
        p_retailprice Decimal(15,2) NOT NULL,
        p_comment     String NOT NULL
) ENGINE = CnchMergeTree() 
ORDER BY p_partkey
CLUSTER BY p_partkey INTO 3 BUCKETS;

create table supplier (
        s_suppkey     Int32 NOT NULL,
        s_name        String NOT NULL,
        s_address     String NOT NULL,
        s_nationkey   Int32 NOT NULL,
        s_phone       String NOT NULL,
        s_acctbal     Decimal(15,2) NOT NULL,
        s_comment     String NOT NULL
) ENGINE = CnchMergeTree() 
ORDER BY s_suppkey
CLUSTER BY s_suppkey INTO 3 BUCKETS;

create table partsupp (
        ps_partkey     Int32 NOT NULL,
        ps_suppkey     Int32 NOT NULL,
        ps_availqty    Int32 NOT NULL,
        ps_supplycost  Decimal(15,2) NOT NULL,
        ps_comment     String NOT NULL
) ENGINE = CnchMergeTree()
ORDER BY ps_partkey
CLUSTER BY ps_partkey INTO 3 BUCKETS;

create table customer (
    c_custkey  Int32,
    c_name  Nullable(String),
    c_address  Nullable(String),
    c_nationkey  Nullable(Int32),
    c_phone  Nullable(String),
    c_acctbal  Nullable(Float64),
    c_mktsegment  Nullable(String),
    c_comment  Nullable(String)
) ENGINE = CnchMergeTree() 
ORDER BY c_custkey
CLUSTER BY c_custkey INTO 3 BUCKETS;

create table orders (
        o_orderkey       Int64 NOT NULL,
        o_custkey        Int32 NOT NULL,
        o_orderstatus    FixedString(1) NOT NULL,
        o_totalprice     Decimal(15,2)  NOT NULL,
        o_orderdate      DATE NOT NULL,
        o_orderpriority  LowCardinality(String NOT NULL),
        o_clerk          String NOT NULL,
        o_shippriority   Int32 NOT NULL,
        o_comment        String NOT NULL
) ENGINE = CnchMergeTree() 
ORDER BY o_orderkey
CLUSTER BY o_orderkey INTO 3 BUCKETS;

create table lineitem (
        l_orderkey    Int64 NOT NULL,
        l_partkey     Int32 NOT NULL,
        l_suppkey     Int32 NOT NULL,
        l_linenumber  Int32 NOT NULL,
        l_quantity    Decimal(15,2) NOT NULL,
        l_extendedprice  Decimal(15,2) NOT NULL,
        l_discount    Decimal(15,2) NOT NULL,
        l_tax         Decimal(15,2) NOT NULL,
        l_returnflag  FixedString(1) NOT NULL,
        l_linestatus  FixedString(1) NOT NULL,
        l_shipdate    DATE NOT NULL,
        l_commitdate  DATE NOT NULL,
        l_receiptdate DATE NOT NULL,
        l_shipinstruct LowCardinality(String NOT NULL),
        l_shipmode     LowCardinality(String NOT NULL),
        l_comment      String NOT NULL
) ENGINE = CnchMergeTree() 
ORDER BY l_orderkey
CLUSTER BY l_orderkey INTO 3 BUCKETS;
