DROP TABLE IF EXISTS `48022_map_key_value_types`;

CREATE TABLE `48022_map_key_value_types` (
    a UInt64,
    m1 Map(String, Array(String)),
    m2 Map(UInt64, Date),
    m3 Map(String, FixedString(20)),
    m4 Map(String, Enum16('a'=1,'b'=2)),
    m5 Map(String, Float64),
    m6 Map(String, LowCardinality(Nullable(Int32)))
) ENGINE = CnchMergeTree order by a;

DROP TABLE IF EXISTS `48022_map_key_value_types`;

CREATE TABLE `48022_map_key_value_types` (
    a UInt64,
    m1 Map(String, Nullable(String))
) ENGINE = CnchMergeTree order by a; -- { serverError 36 }

DROP TABLE IF EXISTS `48022_map_key_value_types`;

CREATE TABLE `48022_map_key_value_types` (
    a UInt64,
    m1 Map(String, LowCardinality(String))
) ENGINE = CnchMergeTree order by a; -- { serverError 36 }

DROP TABLE IF EXISTS `48022_map_key_value_types`;

CREATE TABLE `48022_map_key_value_types` (
    a UInt64,
    m1 Map(Array(String), String)
) ENGINE = CnchMergeTree order by a; -- { serverError 36 }

DROP TABLE IF EXISTS `48022_map_key_value_types`;