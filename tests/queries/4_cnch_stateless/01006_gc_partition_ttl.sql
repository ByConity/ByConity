DROP TABLE IF EXISTS t_gc_partition_ttl;

CREATE TABLE t_gc_partition_ttl(p DateTime, k Int32, m Int32) ENGINE = CnchMergeTree() 
PARTITION BY toDate(p) ORDER BY k TTL p + INTERVAL 3 SECOND SETTINGS old_parts_lifetime = 1;

-- stop background GC thread.
SYSTEM START GC t_gc_partition_ttl;
SYSTEM STOP GC t_gc_partition_ttl;

INSERT INTO t_gc_partition_ttl VALUES (now() - 100, 1, 1);
-- wait the part become outdated by old_parts_lifetime
SELECT sleepEachRow(3) FROM numbers(1) FORMAT Null;

-- run GC manually.
SYSTEM GC t_gc_partition_ttl;

SELECT count() FROM t_gc_partition_ttl;

DROP TABLE t_gc_partition_ttl;

------------------------------------------------

DROP TABLE IF EXISTS t_gc_partition_ttl2;

CREATE TABLE t_gc_partition_ttl2(p Date, t DateTime, k Int32, m Int32) ENGINE = CnchMergeTree() 
PARTITION BY p ORDER BY k TTL t + INTERVAL 3 SECOND SETTINGS old_parts_lifetime = 1;

-- stop background GC thread.
SYSTEM START GC t_gc_partition_ttl2;
SYSTEM STOP GC t_gc_partition_ttl2;

INSERT INTO t_gc_partition_ttl2 VALUES (today(), now() - 100, 1, 1);
-- wait the part become outdated by old_parts_lifetime
SELECT sleepEachRow(3) FROM numbers(1) FORMAT Null;

-- run GC manually.
SYSTEM GC t_gc_partition_ttl2;

SELECT count() FROM t_gc_partition_ttl2;

-- stop background MERGE thread to void TTL Merge.
SYSTEM START MERGES t_gc_partition_ttl2;
SYSTEM STOP MERGES t_gc_partition_ttl2;

-- insert expired rows.
INSERT INTO t_gc_partition_ttl2 VALUES (today(), now() - 100, 1, 1);
-- insert regular rows.
INSERT INTO t_gc_partition_ttl2 VALUES (today(), now() + 100, 1, 1);

SYSTEM GC t_gc_partition_ttl2;

SELECT count() FROM t_gc_partition_ttl2;

DROP TABLE t_gc_partition_ttl2;
