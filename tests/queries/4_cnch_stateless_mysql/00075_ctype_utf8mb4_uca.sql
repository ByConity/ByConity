
-- source include/have_ucs2.inc
-- source include/have_utf8mb4.inc

--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings

--echo #
--echo # Start of 5.5 tests
--echo #

-- SET NAMES utf8mb4;
CREATE TABLE t1 (c1 CHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin);

--source include/ctype_unicode_latin.inc

SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_unicode_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_icelandic_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_latvian_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_romanian_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_slovenian_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_polish_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_estonian_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_spanish_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_swedish_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_turkish_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_czech_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_danish_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_lithuanian_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_slovak_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_spanish2_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_roman_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_esperanto_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_hungarian_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_croatian_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_german2_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_unicode_520_ci;
SELECT GROUP_CONCAT(c1 ORDER BY c1 SEPARATOR '') FROM t1 GROUP BY c1 COLLATE utf8mb4_vietnamese_ci;

DROP TABLE t1;

--echo #
--echo # Start of 5.5 tests
--echo #
--
-- Bug#57737 Character sets: search fails with like, contraction, index
-- Test my_like_range and contractions
--
-- SET collation_connection=utf8mb4_czech_ci;
--source include/ctype_czech.inc
--source include/ctype_like_ignorable.inc

--echo #
--echo # End of 5.5 tests
--echo #
