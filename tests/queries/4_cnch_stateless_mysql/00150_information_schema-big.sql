-- The include statement below is a temp one for tests that are yet to
--be ported to run with InnoDB,
--but needs to be kept for tests that would need MyISAM in future.
--source include/force_myisam_default.inc

-- This test  uses grants, which can't get tested for embedded server
-- source include/big_test.inc
-- source include/not_embedded.inc

--disable_warnings
DROP TABLE IF EXISTS t0,t1,t2,t3,t4,t5;
DROP VIEW IF EXISTS v1;
--enable_warnings


--echo #
--echo # Bug#18925: subqueries with MIN/MAX functions on INFORMARTION_SCHEMA 
--echo #

SELECT t.table_name, c1.column_name
  FROM information_schema.tables t
       INNER JOIN
       information_schema.columns c1
       ON t.table_schema = c1.table_schema AND
          t.table_name = c1.table_name
  WHERE t.table_schema = 'information_schema' AND
        c1.ordinal_position =
        ( SELECT COALESCE(MIN(c2.ordinal_position),1)
            FROM information_schema.columns c2
            WHERE c2.table_schema = t.table_schema AND
                  c2.table_name = t.table_name AND
                  c2.column_name LIKE '%SCHEMA%'
        )
  AND t.table_name NOT LIKE 'ndb%'
  AND t.table_name NOT LIKE 'innodb%';
SELECT t.table_name, c1.column_name
  FROM information_schema.tables t
       INNER JOIN
       information_schema.columns c1
       ON t.table_schema = c1.table_schema AND
          t.table_name = c1.table_name
  WHERE t.table_schema = 'information_schema' AND
        c1.ordinal_position =
        ( SELECT COALESCE(MIN(c2.ordinal_position),1)
            FROM information_schema.columns c2
            WHERE c2.table_schema = 'information_schema' AND
                  c2.table_name = t.table_name AND
                  c2.column_name LIKE '%SCHEMA%'
        )
  AND t.table_name NOT LIKE 'ndb%'
  AND t.table_name NOT LIKE 'innodb%';
