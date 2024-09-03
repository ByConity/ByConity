
--
-- Testing of comments
--

select 1+2/*hello*/+3;
select 1 /* long
multi line comment */;
--error 1065
 ;
select 1 /*!32301 +1 */;
select 1 /*!52301 +1 */;
select 1--1;
-- Note that the following returns 4 while it should return 2
-- This is because the mysqld server doesn't parse -- comments
select 1 --2
+1;
select 1 -- The rest of the row will be ignored
;
/* line with only comment */;

-- End of 4.1 tests

--
-- Bug#25411 (trigger code truncated)
--

--error ER_PARSE_ERROR
select 1/*!2*/;

--error ER_PARSE_ERROR
select 1/*!000002*/;

select 1/*!999992*/;

select 1 + /*!00000 2 */ + 3 /*!99999 noise*/ + 4;

--
-- Bug#28779 (mysql_query() allows execution of statements with unbalanced
-- comments)
--

--disable_warnings
drop table if exists table_28779;
--enable_warnings

