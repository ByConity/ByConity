SELECT toUInt8OrZero(-123), toUInt8OrNull(-123), toUInt8OrZero('123a'), toUInt8OrNull('123a'), toUInt8OrZero('456');
SELECT toUInt16OrZero(-123), toUInt16OrNull(-123), toUInt16OrZero('123a'), toUInt16OrNull('123a'), toUInt16OrZero('456');
SELECT toUInt32OrZero(-123), toUInt32OrNull(-123), toUInt32OrZero('123a'), toUInt32OrNull('123a'), toUInt32OrZero('456');
SELECT toUInt256OrZero(-123), toUInt256OrNull(-123), toUInt256OrZero('123a'), toUInt256OrNull('123a'), toUInt256OrZero('456');

SELECT toInt8OrZero(-123), toInt8OrNull(-123), toInt8OrZero('123a'), toInt8OrNull('123a'), toInt8OrZero('456');
SELECT toInt16OrZero(-123), toInt16OrNull(-123), toInt16OrZero('123a'), toInt16OrNull('123a'), toInt16OrZero('456');
SELECT toInt32OrZero(-123), toInt32OrNull(-123), toInt32OrZero('123a'), toInt32OrNull('123a'), toInt32OrZero('456');
SELECT toInt64OrZero(-123), toInt64OrNull(-123), toInt64OrZero('123a'), toInt64OrNull('123a'), toInt64OrZero('456');
SELECT toInt128OrZero(-123), toInt128OrNull(-123), toInt128OrZero('123a'), toInt128OrNull('123a'), toInt128OrZero('456');
SELECT toInt256OrZero(-123), toInt256OrNull(-123), toInt256OrZero('123a'), toInt256OrNull('123a'), toInt256OrZero('456');

SELECT toFloat32OrZero(-123), toFloat32OrNull(-123), toFloat32OrZero('123a'), toFloat32OrNull('123a'), toFloat32OrZero('456');
SELECT toFloat64OrZero(-123), toFloat64OrNull(-123), toFloat64OrZero('123a'), toFloat64OrNull('123a'), toFloat64OrZero('456');

SELECT toDateOrZero(-123), toDateOrNull(-123), toDateOrZero('123a'), toDateOrNull('123a'), toDateOrZero('456');
-- toDate32OrZero/Null(-123) is '1970-01-01'-123
SELECT toDate32OrZero(-123), toDate32OrNull(-123), toDate32OrZero('123a'), toDate32OrNull('123a'), toDate32OrZero('456');

SELECT toDateTimeOrZero(-123, 'Asia/Chungking'), toDateTimeOrNull(-123, 'Asia/Chungking'), toDateTimeOrZero('123a', 'Asia/Chungking'), toDateTimeOrNull('123a', 'Asia/Chungking'), toDateTimeOrZero('456', 'Asia/Chungking');
-- toDateTime64OrZero/Null(-123, 1, 'Asia/Chungking') is '1970-01-01 08:00:00.0'-123
SELECT toDateTime64OrZero(-123, 1, 'Asia/Chungking'), toDateTime64OrNull(-123, 1, 'Asia/Chungking'), toDateTime64OrZero('123a', 1, 'Asia/Chungking'), toDateTime64OrNull('123a', 1, 'Asia/Chungking'), toDateTime64OrZero('456', 1, 'Asia/Chungking');

SELECT toDecimal32OrZero(-123, 2), toDecimal32OrNull(-123, 2), toDecimal32OrZero('123a', 2), toDecimal32OrNull('123a', 2), toDecimal32OrZero('456', 2);
SELECT toDecimal64OrZero(-123, 2), toDecimal64OrNull(-123, 2), toDecimal64OrZero('123a', 2), toDecimal64OrNull('123a', 2), toDecimal64OrZero('456', 2);
SELECT toDecimal128OrZero(-123, 2), toDecimal128OrNull(-123, 2), toDecimal128OrZero('123a', 2), toDecimal128OrNull('123a', 2), toDecimal128OrZero('456', 2);
SELECT toDecimal256OrZero(-123, 2), toDecimal256OrNull(-123, 2), toDecimal256OrZero('123a', 2), toDecimal256OrNull('123a', 2), toDecimal256OrZero('456', 2);

SELECT toDate(toNullable(toDate32('2022-11-27'))), toDate(toDate32('2022-11-27'));
SELECT toDateTime(toNullable(toDateTime64('2022-11-27 13:27:04.416509', 6, 'Asia/Chungking'))), toDateTime(toDateTime64('2022-11-27 13:27:04.416509', 6, 'Asia/Chungking'));
SELECT toDateTime64(toNullable(toDateTime('2022-11-27 13:27:04', 'Asia/Chungking')), 6), toDateTime64(toDateTime('2022-11-27 13:27:04', 'Asia/Chungking'), 6);

SELECT toDate(toDateTime64OrZero(CURTIME())) = today();
SELECT toDate(toDateTime64OrNull(CURTIME())) = today();