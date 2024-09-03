
--  #Get deafult engine value
--let $DEFAULT_ENGINE = `select @@global.default_storage_engine`

--
-- Test for various CREATE statements and character sets
--


-- Check that the database charset is taken from server charset by default:
-- - Change local character_set_server variable to latin5.
-- - Create database with and without CHARACTER SET specification.
-- At the same time check fix for the
-- Bug#2151:
-- "USE db" with non-default character set should never affect 
-- further CREATE DATABASEs.


-- SET @@character_set_server=latin5;
CREATE DATABASE mysqltest1;
USE mysqltest1;
CREATE DATABASE mysqltest2;

--
-- This should be cp1251
--
SHOW CREATE DATABASE mysqltest1;

--
-- Database "mysqltest2" should take the default latin5 value from
-- the server level.
-- Afterwards, table "d2.t1" should inherit the default latin5 value from
-- the database "mysqltest2", using database option hash.
--
SHOW CREATE DATABASE mysqltest2;
CREATE TABLE mysqltest2.t1 (a char(10));

--Replace default engine value with static engine string 
--replace_result $DEFAULT_ENGINE ENGINE
SHOW CREATE TABLE mysqltest2.t1;
DROP TABLE mysqltest2.t1;

--
-- Now we check if the database charset is updated in
-- the database options hash when we ALTER DATABASE.
--
CREATE TABLE mysqltest2.t1 (a char(10));

--Replace default engine value with static engine string 
--replace_result $DEFAULT_ENGINE ENGINE
SHOW CREATE TABLE mysqltest2.t1;
DROP DATABASE mysqltest2;

--
-- Now we check if the database charset is removed from
-- the database option hash when we DROP DATABASE.
--
CREATE DATABASE mysqltest2;
CREATE TABLE mysqltest2.t1 (a char(10));

--Replace default engine value with static engine string 
--replace_result $DEFAULT_ENGINE ENGINE
SHOW CREATE TABLE mysqltest2.t1;
DROP DATABASE mysqltest2;

--
-- Check that table value uses database level by default
--
USE mysqltest1;
CREATE TABLE t1 (a char(10));

--Replace default engine value with static engine string 
--replace_result $DEFAULT_ENGINE ENGINE
SHOW CREATE TABLE t1;
DROP TABLE t1;

--
-- Bug#3255
--
CREATE TABLE t1 (a char(10));

--Replace default engine value with static engine string 
--replace_result $DEFAULT_ENGINE ENGINE
SHOW CREATE TABLE t1;
DROP TABLE t1;
CREATE TABLE t1 (a char(10));

--Replace default engine value with static engine string 
--replace_result $DEFAULT_ENGINE ENGINE
SHOW CREATE TABLE t1;
DROP TABLE t1;

DROP DATABASE mysqltest1;
