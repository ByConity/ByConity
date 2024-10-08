drop table if exists tealimit_ansi;
CREATE TABLE tealimit_ansi
(
 `id` UInt64,
 `event` String,
 `event_date` Date,
 `string_params` Map(String, UInt64),
 `float_params` Map(String, Float64),
 `int_params` Map(String, Int8)
)
ENGINE = CnchMergeTree
ORDER BY id;

insert into tealimit_ansi values (6, 'app_change', '2018-08-15', {'user':12},{'amount':12.34, 'amount':23.56, 'amount_1':12, 'amount_1':20},{'abc':1}), (6, 'app_change', '2018-08-16', {'user1':12},{'amount':12.34},{'abc':1});

set dialect_type='ANSI';
SELECT
 event_date AS t,
 string_params{'user'} AS g0,
 string_params{'user1'} AS g1,
 max(float_params{'amount'}) AS amount,
 max(float_params{'amount_1'}) AS amount1
FROM tealimit_ansi
WHERE (((id = 6) AND (event_date >= '2018-08-15')) AND (event_date <= '2018-09-13')) AND (event = 'app_change')
GROUP BY
 t,
 g0,
 g1
ORDER BY t,g0,g1
TEALIMIT 200 GROUP (assumeNotNull(g0), g1) ORDER (amount, amount1) DESC;

drop table if exists tealimit_ansi