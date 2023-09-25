CREATE TABLE q1_t1
(
    a UInt64,
    b UInt64
) ENGINE = Memory();

CREATE TABLE q2_t1
(
    a UInt64,
    b UInt64
) ENGINE = Memory();

CREATE TABLE q3_t1
(
    q3_id UInt64,
    q3_date Date
) ENGINE = CnchMergeTree()
ORDER BY q3_id;

CREATE TABLE q4_t1
(
    hash_uid UInt64,
    cohort_id Int64,
    app_id Int64,
    q4_date Date
) ENGINE = CnchMergeTree()
ORDER BY hash_uid;

CREATE TABLE q5_t1
(
    a Int64,
    b Float64
) ENGINE = CnchMergeTree()
ORDER BY a;

CREATE TABLE q6_t1
(
    a UInt64,
    b UInt64
) ENGINE = Memory();

CREATE TABLE q6_t2
(
    a UInt64,
    c UInt64
) ENGINE = Memory();

CREATE TABLE q6_t3
(
    a UInt64,
    d UInt64
) ENGINE = Memory();

CREATE TABLE q6_t4
(
    a UInt64,
    e UInt64
) ENGINE = Memory();

CREATE TABLE q6_t5
(
    a UInt64,
    f UInt64
) ENGINE = Memory();

CREATE TABLE q15_t1
(
    a UInt64,
    b UInt64
) ENGINE = Memory();

CREATE TABLE q15_t2
(
    a UInt64,
    b UInt64
) ENGINE = Memory();

CREATE TABLE q16_t1
(
    a UInt64,
    b UInt64
) ENGINE = Memory();

CREATE TABLE q16_t2
(
    a UInt64,
    b UInt64
) ENGINE = Memory();

CREATE TABLE q17_t1
(
    a UInt64,
    b UInt64
) ENGINE = Memory();

CREATE TABLE q17_t2
(
    a UInt64,
    b UInt64
) ENGINE = Memory();

CREATE TABLE q17_t3
(
    a UInt64,
    b UInt64
) ENGINE = Memory();
