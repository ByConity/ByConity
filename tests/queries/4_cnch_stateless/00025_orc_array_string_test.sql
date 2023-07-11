DROP TABLE IF EXISTS orc_array_string_test;

CREATE TABLE orc_array_string_test
(
    a1 Array(String),
    a2 Array(FixedString(10)),
    date String
)
ENGINE = CnchHive(`data.olap.cnch_hms.service.lf`, `cnch_hive_external_table`, `orc_array_string_type`)
PARTITION BY (date);

select * from orc_array_string_test order by date, a1;

DROP TABLE IF EXISTS orc_array_string_test;
