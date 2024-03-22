
drop table if exists `test`.`testNullableGroupArray`;
CREATE TABLE `test`.`testNullableGroupArray`
(
    `t` Date,
    `price` Nullable(Float64)
)
ENGINE = MergeTree
PRIMARY KEY t
ORDER BY t;

set group_array_can_return_nullable = 1;
SELECT toTypeName(groupArray(price)) AS total_prices
FROM
(
    SELECT price
    FROM `test`.`testNullableGroupArray`
);

set group_array_can_return_nullable = 0;
SELECT toTypeName(groupArray(price)) AS total_prices
FROM
(
    SELECT price
    FROM `test`.`testNullableGroupArray`
);

