DROP TABLE IF EXISTS sales_transaction;
DROP TABLE IF EXISTS items;

CREATE TABLE sales_transaction  (`store_code` Nullable(Int64), `item_code` Nullable(String), `transactiondate` Date, `transaction_time` Nullable(DateTime), `quantity` Nullable(Int64), `extended_price` Nullable(Float64), `salestransactionid` Nullable(String)) ENGINE = CnchMergeTree PARTITION BY toDate(toStartOfWeek(`transactiondate`)) PRIMARY KEY `transactiondate` ORDER BY `transactiondate` SETTINGS index_granularity = 8192;
CREATE TABLE items  (`category_id` Nullable(String), `category_name` Nullable(String), `subcat_id` Nullable(String), `subcat_name` Nullable(String), `ean` Nullable(String), `item_code` String, `long_name` Nullable(String), `moq` Nullable(Float64), `brand` Nullable(String), `sub_brand` Nullable(String), `width` Nullable(String), `height` Nullable(String), `depth` Nullable(String), `dim_type` Nullable(String), `item_status` Nullable(String), `available_date` Nullable(Date), `cost` Nullable(Float64), `price` Nullable(Float64)) ENGINE = CnchMergeTree PRIMARY KEY `item_code` ORDER BY `item_code` SETTINGS index_granularity = 8192;

set enable_optimizer=1;
explain SELECT `s`.`item_code`
FROM `sales_transaction` AS s
         INNER JOIN `items` AS i ON `i`.`item_code` = `s`.`item_code`
WHERE toStartOfMonth(`transaction_time`) - toStartOfMonth(`transaction_time`) = toStartOfMonth(toDate(now())) + toIntervalWeek(1) - toStartOfMonth(toDate(now()))