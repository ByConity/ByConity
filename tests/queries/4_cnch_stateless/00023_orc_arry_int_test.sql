DROP TABLE IF EXISTS orc_array_int_test;

CREATE TABLE orc_array_int_test
(
    a1 Array(tinyint),
    a2 Array(smallint),
    a3 Array(int),
    a4 Array(bigint),
    a6 Array(float),
    a7 Array(double),
    date String)
ENGINE = CnchHive(`data.olap.cnch_hms.service.lf`, `cnch_hive_external_table`, `orc_array_int_type`)
PARTITION BY (date);

select * from orc_array_int_test order by date;

DROP TABLE IF EXISTS orc_array_int_test;
