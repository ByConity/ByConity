DROP TABLE IF EXISTS num_d;
DROP TABLE IF EXISTS num;

CREATE TABLE num(number UInt64) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO num SELECT number FROM system.numbers limit 10;
CREATE TABLE num_d AS num ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'num');

SELECT
    number,
    grouping(number, number % 2, number % 3) = 6
FROM num_d
GROUP BY
    number,
    number % 2
ORDER BY number; -- { serverError 36 }

-- { echoOn }
SELECT
    number,
    grouping(number, number % 2) = 3
FROM num_d
GROUP BY
    number,
    number % 2
ORDER BY number;

SELECT
    number,
    grouping(number),
    GROUPING(number % 2)
FROM num_d
GROUP BY
    number,
    number % 2
ORDER BY number;

SELECT
    number,
    grouping(number, number % 2) AS gr
FROM num_d
GROUP BY
    number,
    number % 2
WITH ROLLUP
ORDER BY
    number, gr
SETTINGS enable_optimizer=0;

SELECT
    number,
    grouping(number, number % 2) AS gr
FROM num_d
GROUP BY
    ROLLUP(number, number % 2)
ORDER BY
    number, gr
SETTINGS enable_optimizer=0;

SELECT
    number,
    grouping(number, number % 2) AS gr
FROM num_d
GROUP BY
    number,
    number % 2
WITH CUBE
ORDER BY
    number, gr
SETTINGS enable_optimizer=0;

SELECT
    number,
    grouping(number, number % 2) AS gr
FROM num_d
GROUP BY
    CUBE(number, number % 2)
ORDER BY
    number, gr
SETTINGS enable_optimizer=0;

SELECT
    number,
    grouping(number, number % 2) + 3 as gr
FROM num_d
GROUP BY
    CUBE(number, number % 2)
HAVING grouping(number) != 0
ORDER BY
    number, gr
SETTINGS enable_optimizer=0;

SELECT
    number,
    grouping(number, number % 2) as gr
FROM num_d
GROUP BY
    CUBE(number, number % 2) WITH TOTALS
HAVING grouping(number) != 0
ORDER BY
    number, gr
SETTINGS enable_optimizer=0; -- { serverError 10 }

SELECT
    number,
    grouping(number, number % 2) as gr
FROM num_d
GROUP BY
    CUBE(number, number % 2) WITH TOTALS
ORDER BY
    number, gr
SETTINGS enable_optimizer=0;

SELECT
    number,
    grouping(number, number % 2) as gr
FROM num_d
GROUP BY
    ROLLUP(number, number % 2) WITH TOTALS
HAVING grouping(number) != 0
ORDER BY
    number, gr
SETTINGS enable_optimizer=0;; -- { serverError 10 }

SELECT
    number,
    grouping(number, number % 2) as gr
FROM num_d
GROUP BY
    ROLLUP(number, number % 2) WITH TOTALS
ORDER BY
    number, gr
SETTINGS enable_optimizer=0;

-- { echoOff }
DROP TABLE IF EXISTS num_d;
DROP TABLE IF EXISTS num;
