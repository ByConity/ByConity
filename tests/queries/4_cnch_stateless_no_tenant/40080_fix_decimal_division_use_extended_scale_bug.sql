DROP TABLE IF EXISTS t40080_dec_div;
CREATE TABLE t40080_dec_div (`a` Decimal(7, 2), b Int32, `c` Decimal(7, 2), `d` Decimal(7, 3)) engine=CnchMergeTree order by a;
insert into t40080_dec_div values (1, 1, 1, 1);

SELECT a/b, b/a, a/c, a/d, d/a from t40080_dec_div SETTINGS decimal_division_use_extended_scale = 1;

