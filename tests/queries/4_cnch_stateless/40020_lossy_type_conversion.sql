-- { echo }
SELECT 1::UInt64 UNION ALL SELECT 1::Float64 SETTINGS allow_extended_type_conversion = 0; -- { serverError 386 }
SELECT 1::UInt64 UNION ALL SELECT 1::Int32 SETTINGS allow_extended_type_conversion = 0; -- { serverError 386 }
SELECT 1::Decimal32(2) UNION ALL SELECT 1::Float64 SETTINGS allow_extended_type_conversion = 0; -- { serverError 386 }
SELECT * FROM (SELECT 1::UInt64 AS a) JOIN (SELECT 1::Float64 AS a) USING a SETTINGS allow_extended_type_conversion = 0; -- { serverError 53 }
SELECT * FROM (SELECT 1::UInt64 AS a) JOIN (SELECT 1::Int32 AS a) USING a SETTINGS allow_extended_type_conversion = 0; -- { serverError 53 }
SELECT * FROM (SELECT 1::Decimal32(2) AS a) JOIN (SELECT 1::Float64 AS a) USING a SETTINGS allow_extended_type_conversion = 0; -- { serverError 53 }

SELECT 1::UInt64 UNION ALL SELECT 1::Float64 SETTINGS allow_extended_type_conversion = 1;
SELECT 1::UInt64 UNION ALL SELECT 1::Int32 SETTINGS allow_extended_type_conversion = 1;
SELECT 1::Decimal32(2) UNION ALL SELECT 1::Float64 SETTINGS allow_extended_type_conversion = 1;
SELECT * FROM (SELECT 1::UInt64 AS a) JOIN (SELECT 1::Float64 AS a) USING a SETTINGS allow_extended_type_conversion = 1;
SELECT * FROM (SELECT 1::UInt64 AS a) JOIN (SELECT 1::Int32 AS a) USING a SETTINGS allow_extended_type_conversion = 1;
SELECT * FROM (SELECT 1::Decimal32(2) AS a) JOIN (SELECT 1::Float64 AS a) USING a SETTINGS allow_extended_type_conversion = 1;

