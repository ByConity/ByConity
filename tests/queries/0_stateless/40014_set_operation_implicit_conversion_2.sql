SET enable_optimizer=1;
(SELECT toUInt8(1), toUInt16(2), toUInt8(3), toUInt8(4)) INTERSECT DISTINCT (SELECT toUInt8(1), toUInt8(2), toInt16(3), toInt8(4));
(SELECT toUInt8(1), toUInt16(2), toUInt8(3), toUInt8(4)) EXCEPT DISTINCT (SELECT toUInt8(11), toUInt8(12), toInt16(13), toInt8(14));
