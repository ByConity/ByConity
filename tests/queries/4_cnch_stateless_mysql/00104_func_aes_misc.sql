--echo ##############################################
--echo #### Tests for ECB Mode
--echo ##############################################

--echo "--------------------------------------------"
-- let $block_mode=aes-128-ecb;
-- --echo ##################### Testing with mode : $block_mode
-- eval SET SESSION block_encryption_mode="$block_mode";
-- SELECT @@global.block_encryption_mode;
-- SELECT @@session.block_encryption_mode;

--echo ####Test without tables
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc'), 'cccccccccccccccc')='dddddddddddddddd';
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', 'abcd1234efgh5678'), 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd';

--echo ####Test with InnoDB tables
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(f1 varchar(256));
INSERT INTO t1 values('dddddddddddddddd');
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc'), 'cccccccccccccccc')='dddddddddddddddd' FROM t1;
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', 'abcd1234efgh5678'), 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd' FROM t1;

--echo ####Test with MyISAM tables
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(f1 varchar(256)) engine=MyISAM;
INSERT INTO t1 values('dddddddddddddddd');
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc'), 'cccccccccccccccc')='dddddddddddddddd' FROM t1;
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', 'abcd1234efgh5678'), 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd' FROM t1;

--echo "--------------------------------------------"
-- let $block_mode=aes-192-ecb;
--echo ##################### Testing with mode : $block_mode
-- eval SET SESSION block_encryption_mode="$block_mode";
-- SELECT @@global.block_encryption_mode;
-- SELECT @@session.block_encryption_mode;

--echo ####Test without tables
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc'), 'cccccccccccccccc')='dddddddddddddddd';
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', 'abcd1234efgh5678'), 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd';

--echo ####Test with InnoDB tables
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(f1 varchar(256));
INSERT INTO t1 values('dddddddddddddddd');
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc'), 'cccccccccccccccc')='dddddddddddddddd' FROM t1;
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', 'abcd1234efgh5678'), 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd' FROM t1;

--echo ####Test with MyISAM tables
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(f1 varchar(256)) engine=MyISAM;
INSERT INTO t1 values('dddddddddddddddd');
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc'), 'cccccccccccccccc')='dddddddddddddddd' FROM t1;
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', 'abcd1234efgh5678'), 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd' FROM t1;
--echo "--------------------------------------------"
-- let $block_mode=aes-256-ecb;
-- --echo ##################### Testing with mode : $block_mode
-- eval SET SESSION block_encryption_mode="$block_mode";
-- --echo should return 1
-- SELECT @@global.block_encryption_mode;
-- --echo should return 1
-- SELECT @@session.block_encryption_mode;

--echo ####Test without tables
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc'), 'cccccccccccccccc')='dddddddddddddddd';
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', 'abcd1234efgh5678'), 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd';

--echo ####Test with InnoDB tables
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(f1 varchar(256));
INSERT INTO t1 values('dddddddddddddddd');
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc'), 'cccccccccccccccc')='dddddddddddddddd' FROM t1;
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', 'abcd1234efgh5678'), 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd' FROM t1;

--echo ####Test with MyISAM tables
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(f1 varchar(256)) engine=MyISAM;
INSERT INTO t1 values('dddddddddddddddd');
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc'), 'cccccccccccccccc')='dddddddddddddddd' FROM t1;
--echo should return 1
SELECT AES_DECRYPT(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', 'abcd1234efgh5678'), 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd' FROM t1;
DROP TABLE IF EXISTS t1;
--echo "--------------------------------------------"

--echo ##############################################
--echo "Tests related to RANDOM_BYTES()"
--echo ##############################################
--echo should return 1
select LENGTH(RANDOM_BYTES(1))=1;
--echo should return 1
select LENGTH(RANDOM_BYTES(1024))=1024;
-- # SET 'cccccccccccccccc'=RANDOM_BYTES(1);
-- # SET 'cccccccccccccccc'=RANDOM_BYTES(1024);

--error 1690
select RANDOM_BYTES(0);

--error 1690
select RANDOM_BYTES(1025);
--echo ##############################################
--echo "Tests related to boundary values of IV"
--echo ##############################################
-- # SET 'abcd1234efgh5678'='abcdefghijklmnophelloworldworldisgreat';
-- # SET @IV1='abcdefghijklmnopqrstuvwxyz';
-- # SET 'cccccccccccccccc'='helloworld';
-- # SET 'dddddddddddddddd'=REPEAT('K',100);
-- # SET @@session.block_encryption_mode = 'aes-256-cbc';

--echo should return 1
select AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', 'abcd1234efgh5678')=AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', @IV1);
--echo ##############################################
--echo "Few negative tests with invalid/different keys and IV"
--echo ##############################################
-- # SET 'abcd1234efgh5678'='ijkl8765mnop2345';
-- # SET 'cccccccccccccccc'='helloworld1234567890';
-- # SET 'dddddddddddddddd'=REPEAT('J',255);

-- # SET @@session.block_encryption_mode = 'aes-128-ecb';

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(f1 varchar(256));
INSERT INTO t1 values(AES_ENCRYPT('dddddddddddddddd', 'cccccccccccccccc', 'abcd1234efgh5678'));

--echo Combination1..............
-- # SET @@session.block_encryption_mode = 'aes-192-ecb';
--echo should return NULL
SELECT AES_DECRYPT(f1, 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd' FROM t1;

--echo Combination2..............
-- # SET @@session.block_encryption_mode = 'aes-256-ecb';
--echo should return NULL
SELECT AES_DECRYPT(f1, 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd' FROM t1;

--echo Combination3..............
-- # SET @@session.block_encryption_mode = 'aes-128-cbc';
--echo should return 0 or NULL
SELECT COALESCE (AES_DECRYPT(f1, 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd',0) FROM t1;

--echo Combination4..............
-- # SET @@session.block_encryption_mode = 'aes-192-cbc';
--echo should return NULL
SELECT AES_DECRYPT(f1, 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd' FROM t1;

--echo Combination5..............
-- # SET @@session.block_encryption_mode = 'aes-256-cbc';
--echo should return NULL
SELECT AES_DECRYPT(f1, 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd' FROM t1;

--echo Combination6..............
-- # SET @@session.block_encryption_mode = 'aes-128-ecb';
--echo should return 1
SELECT AES_DECRYPT(f1, 'cccccccccccccccc', 'abcd1234efgh5678')='dddddddddddddddd' FROM t1;

-- # SET @@session.block_encryption_mode = DEFAULT;
DROP TABLE IF EXISTS t1;


--echo #
--echo # End of 5.7 tests
--echo #
