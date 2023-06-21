DROP TABLE IF EXISTS thread_pool_dump;

CREATE TABLE thread_pool_dump (`EventDate` Date, `OrderID` Int32) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY OrderID;

INSERT INTO thread_pool_dump SELECT toDate('2020-08-18'), number from numbers(1048576 * 10) SETTINGS max_threads_for_cnch_dump = 4;

SELECT count() from thread_pool_dump;

DROP TABLE thread_pool_dump;
