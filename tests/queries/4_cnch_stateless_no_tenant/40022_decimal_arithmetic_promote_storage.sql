-- { echo }

-- Decimal op Decimal, storage promotion happens
SELECT 1.2::Decimal32(2) * 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) * 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal32(2) * 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) * 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal32(5) * 0.6::Decimal32(6) SETTINGS decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 0; -- { serverError 69 }
SELECT 1.2::Decimal32(5) * 0.6::Decimal32(6) SETTINGS decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal32(2) / 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) / 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal32(2) / 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) / 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal32(7) / 0.6::Decimal32(3) SETTINGS decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 0; -- { serverError 407 }
SELECT 1.2::Decimal32(7) / 0.6::Decimal32(3) SETTINGS decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal128(2) * 0.6::Decimal128(1), toTypeName(1.2::Decimal128(2) * 0.6::Decimal128(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal128(2) * 0.6::Decimal128(1), toTypeName(1.2::Decimal128(2) * 0.6::Decimal128(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal128(20) * 0.6::Decimal128(19) SETTINGS decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 0;  -- { serverError 69 }
SELECT 1.2::Decimal128(20) * 0.6::Decimal128(19) SETTINGS decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal128(2) / 0.6::Decimal128(1), toTypeName(1.2::Decimal128(2) / 0.6::Decimal128(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal128(2) / 0.6::Decimal128(1), toTypeName(1.2::Decimal128(2) / 0.6::Decimal128(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal128(20) / 0.6::Decimal128(19) SETTINGS decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 0;  -- { serverError 407 }
SELECT 1.2::Decimal128(20) / 0.6::Decimal128(19) SETTINGS decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 1;

-- Decimal op Decimal, storage promotion doesn't happens
SELECT 1.2::Decimal64(2) * 0.6::Decimal64(1), toTypeName(1.2::Decimal64(2) * 0.6::Decimal64(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal64(2) * 0.6::Decimal64(1), toTypeName(1.2::Decimal64(2) * 0.6::Decimal64(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal64(2) / 0.6::Decimal64(1), toTypeName(1.2::Decimal64(2) / 0.6::Decimal64(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal64(2) / 0.6::Decimal64(1), toTypeName(1.2::Decimal64(2) / 0.6::Decimal64(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal32(2) + 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) + 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal32(2) + 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) + 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal32(2) - 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) - 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal32(2) - 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) - 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 1;

-- Decimal op Integral, storage promotion happens
SELECT 1.2::Decimal32(2) / 6::Int32, toTypeName(1.2::Decimal32(2) / 6::Int32) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal32(2) / 6::Int32, toTypeName(1.2::Decimal32(2) / 6::Int32) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 1200000.0::Decimal32(1) / 6::Int32 SETTINGS decimal_division_use_extended_scale = 1, decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 0;  -- { serverError 407 }
SELECT 1200000.0::Decimal32(1) / 6::Int32 SETTINGS decimal_division_use_extended_scale = 1, decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 1;
SELECT 12::Int32 / 0.6::Decimal32(1), toTypeName(12::Int32 / 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 12::Int32 / 0.6::Decimal32(1), toTypeName(12::Int32 / 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 12::Int32 / 0.6::Decimal32(5) SETTINGS decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 0;  -- { serverError 407 }
SELECT 12::Int32 / 0.6::Decimal32(5) SETTINGS decimal_check_overflow = 1, decimal_arithmetic_promote_storage = 1;

-- Decimal op Integral, storage promotion doesn't happens
SELECT 1.2::Decimal64(2) / 6::Int32, toTypeName(1.2::Decimal64(2) / 6::Int32) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal64(2) / 6::Int32, toTypeName(1.2::Decimal64(2) / 6::Int32) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 12::Int32 / 0.6::Decimal64(1), toTypeName(12::Int32 / 0.6::Decimal64(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 12::Int32 / 0.6::Decimal64(1), toTypeName(12::Int32 / 0.6::Decimal64(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal32(2) + 6::Int32, toTypeName(1.2::Decimal32(2) + 6::Int32) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal32(2) + 6::Int32, toTypeName(1.2::Decimal32(2) + 6::Int32) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal32(2) - 6::Int32, toTypeName(1.2::Decimal32(2) - 6::Int32) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal32(2) - 6::Int32, toTypeName(1.2::Decimal32(2) - 6::Int32) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 1.2::Decimal32(2) * 6::Int32, toTypeName(1.2::Decimal32(2) * 6::Int32) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 1.2::Decimal32(2) * 6::Int32, toTypeName(1.2::Decimal32(2) * 6::Int32) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 12::Int32 + 0.6::Decimal32(1), toTypeName(12::Int32 + 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 12::Int32 + 0.6::Decimal32(1), toTypeName(12::Int32 + 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 12::Int32 - 0.6::Decimal32(1), toTypeName(12::Int32 - 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 12::Int32 - 0.6::Decimal32(1), toTypeName(12::Int32 - 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
SELECT 12::Int32 * 0.6::Decimal32(1), toTypeName(12::Int32 * 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 0;
SELECT 12::Int32 * 0.6::Decimal32(1), toTypeName(12::Int32 * 0.6::Decimal32(1)) SETTINGS decimal_arithmetic_promote_storage = 1;
