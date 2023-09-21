-- { echo }

-- Decimal / Decimal
SELECT 1.2::Decimal32(2) / 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) / 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal32(2) / 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) / 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal32(2) / 0.6::Decimal32(2), toTypeName(1.2::Decimal32(2) / 0.6::Decimal32(2)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal32(2) / 0.6::Decimal32(2), toTypeName(1.2::Decimal32(2) / 0.6::Decimal32(2)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal32(2) / 6::Decimal32(0), toTypeName(1.2::Decimal32(2) / 6::Decimal32(0)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal32(2) / 6::Decimal32(0), toTypeName(1.2::Decimal32(2) / 6::Decimal32(0)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal32(7) / 0.6::Decimal32(1), toTypeName(1.2::Decimal32(7) / 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal32(7) / 0.6::Decimal32(1), toTypeName(1.2::Decimal32(7) / 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal64(7) / 0.6::Decimal64(7), toTypeName(1.2::Decimal64(7) / 0.6::Decimal64(7)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal64(7) / 0.6::Decimal64(7), toTypeName(1.2::Decimal64(7) / 0.6::Decimal64(7)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal64(2) / 0.6::Decimal32(1), toTypeName(1.2::Decimal64(2) / 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal64(2) / 0.6::Decimal32(1), toTypeName(1.2::Decimal64(2) / 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal64(7) / 0.6::Decimal32(1), toTypeName(1.2::Decimal64(7) / 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal64(7) / 0.6::Decimal32(1), toTypeName(1.2::Decimal64(7) / 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal32(2) / 0.6::Decimal64(1), toTypeName(1.2::Decimal32(2) / 0.6::Decimal64(1)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal32(2) / 0.6::Decimal64(1), toTypeName(1.2::Decimal32(2) / 0.6::Decimal64(1)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal32(7) / 0.6::Decimal64(1), toTypeName(1.2::Decimal32(7) / 0.6::Decimal64(1)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal32(7) / 0.6::Decimal64(1), toTypeName(1.2::Decimal32(7) / 0.6::Decimal64(1)) SETTINGS decimal_division_use_extended_scale = 1;

-- Decimal / Integral
SELECT 1.2::Decimal32(2) / 6::Int32, toTypeName(1.2::Decimal32(2) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal32(2) / 6::Int32, toTypeName(1.2::Decimal32(2) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal32(7) / 6::Int32, toTypeName(1.2::Decimal32(7) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal32(7) / 6::Int32, toTypeName(1.2::Decimal32(7) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 12::Decimal32(0) / 6::Int32, toTypeName(12::Decimal32(0) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 12::Decimal32(0) / 6::Int32, toTypeName(12::Decimal32(0) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal64(2) / 6::Int32, toTypeName(1.2::Decimal64(2) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal64(2) / 6::Int32, toTypeName(1.2::Decimal64(2) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal64(7) / 6::Int32, toTypeName(1.2::Decimal64(7) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal64(7) / 6::Int32, toTypeName(1.2::Decimal64(7) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 12::Decimal64(0) / 6::Int32, toTypeName(12::Decimal64(0) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 12::Decimal64(0) / 6::Int32, toTypeName(12::Decimal64(0) / 6::Int32) SETTINGS decimal_division_use_extended_scale = 1;

-- Integral / Decimal
SELECT 12::Int32 / 0.6::Decimal32(1), toTypeName(12::Int32 / 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 12::Int32 / 0.6::Decimal32(1), toTypeName(12::Int32 / 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 12::Int32 / 6::Decimal32(0), toTypeName(12::Int32 / 6::Decimal32(0)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 12::Int32 / 6::Decimal32(0), toTypeName(12::Int32 / 6::Decimal32(0)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 12::Int32 / 0.6::Decimal64(1), toTypeName(12::Int32 / 0.6::Decimal64(1)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 12::Int32 / 0.6::Decimal64(1), toTypeName(12::Int32 / 0.6::Decimal64(1)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 12::Int32 / 0.6::Decimal64(7), toTypeName(12::Int32 / 0.6::Decimal64(7)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 12::Int32 / 0.6::Decimal64(7), toTypeName(12::Int32 / 0.6::Decimal64(7)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 12::Int32 / 6::Decimal64(0), toTypeName(12::Int32 / 6::Decimal64(0)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 12::Int32 / 6::Decimal64(0), toTypeName(12::Int32 / 6::Decimal64(0)) SETTINGS decimal_division_use_extended_scale = 1;

-- other operations should not be affected
SELECT 1.2::Decimal32(2) + 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) + 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal32(2) + 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) + 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal32(2) - 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) - 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal32(2) - 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) - 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal32(2) * 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) * 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal32(2) * 0.6::Decimal32(1), toTypeName(1.2::Decimal32(2) * 0.6::Decimal32(1)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal64(7) + 0.6::Decimal64(7), toTypeName(1.2::Decimal64(7) + 0.6::Decimal64(7)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal64(7) + 0.6::Decimal64(7), toTypeName(1.2::Decimal64(7) + 0.6::Decimal64(7)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal64(7) - 0.6::Decimal64(7), toTypeName(1.2::Decimal64(7) - 0.6::Decimal64(7)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal64(7) - 0.6::Decimal64(7), toTypeName(1.2::Decimal64(7) - 0.6::Decimal64(7)) SETTINGS decimal_division_use_extended_scale = 1;
SELECT 1.2::Decimal64(7) * 0.6::Decimal64(7), toTypeName(1.2::Decimal64(7) * 0.6::Decimal64(7)) SETTINGS decimal_division_use_extended_scale = 0;
SELECT 1.2::Decimal64(7) * 0.6::Decimal64(7), toTypeName(1.2::Decimal64(7) * 0.6::Decimal64(7)) SETTINGS decimal_division_use_extended_scale = 1;
