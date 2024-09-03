-------------------------------------------------------------------------------
-- csv_not_null.test - .test file for MySQL regression suite
-- Purpose:  To test the behavior of the CSV engine
--           Bug#31473 resulted in strict enforcement of non-nullable
--           columns in CSV engine.
-- NOTE:     Main functionality tested - NOT NULL restrictions on CSV tables
--           CREATE, INSERT, and UPDATE statements 
--           ALTER statements in separate file due to BUG#33696 
-- Author pcrews
-- Last modified:  2008-01-04
-------------------------------------------------------------------------------

--############################################################################
-- Testcase csv_not_null.1:  CREATE TABLE for CSV Engine requires explicit
--                           NOT NULL for each column
--############################################################################
-- echo # ===== csv_not_null.1 =====
-- disable_warnings
DROP TABLE IF EXISTS t1, t2;
--enable_warnings

-- SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
CREATE TABLE t1 (a int NOT NULL, b blob NOT NULL, c CHAR(20) NOT NULL, 
d VARCHAR(20) NOT NULL, e enum('foo','bar') NOT NULL,f DATE NOT NULL) 
ENGINE = CSV;
-- echo # === should result in default for each datatype ===
-- disable_warnings
INSERT INTO t1 VALUES();
-- enable_warnings
SELECT * FROM t1;

-- disable_warnings
-- Bug#33717 - INSERT...(default) fails for enum. 
INSERT INTO t1 VALUES(default,default,default,default,default,default);
-- enable_warnings

SELECT * FROM t1;
INSERT INTO t1 VALUES(0,'abc','def','ghi','bar','1999-12-31');
SELECT * FROM t1;

DROP TABLE t1;

--#############################################################################
-- Testcase csv_not_null.3:  UPDATE tests -- examining behavior of UPDATE
--                           statements for CSV
--#############################################################################
-- echo # ===== csv_not_null.3 =====
-- disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings


CREATE TABLE t1 (a int NOT NULL, b char(10) NOT NULL) ENGINE = CSV;
--disable_warnings
INSERT INTO t1 VALUES();
--enable_warnings
SELECT * FROM t1;
--disable_warnings
UPDATE t1 set b = 'new_value' where a = 0;
--enable_warnings
SELECT * FROM t1;
UPDATE t1 set b = NULL where b = 'new_value';
SELECT * FROM t1;

DROP TABLE t1;
-- SET sql_mode = default;
