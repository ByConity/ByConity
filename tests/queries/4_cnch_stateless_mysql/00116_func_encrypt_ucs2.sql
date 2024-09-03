
-- source include/have_ssl_crypto_functs.inc
-- source include/have_ucs2.inc

--echo #
--echo # Bug#59648 my_strtoll10_mb2: Assertion `(*endptr - s) % 2 == 0' failed.
--echo #

SELECT CHAR_LENGTH(DES_ENCRYPT(0, CHAR('1')));
--disable_ps_protocol
SELECT CONVERT(DES_ENCRYPT(0, CHAR('1')),UNSIGNED);
--enable_ps_protocol

SELECT CHAR_LENGTH(DES_DECRYPT(0xFF0DC9FC9537CA75F4, CHAR('1')));
--disable_ps_protocol
SELECT CONVERT(DES_DECRYPT(0xFF0DC9FC9537CA75F4, CHAR('1')), UNSIGNED);
--enable_ps_protocol
