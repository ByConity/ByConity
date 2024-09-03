-- This tests bitwise operations focusing mostly on cases where
-- one of the parameters is binary/varbinary
drop table if exists t;
CREATE TABLE t(id int, vbin1 varbinary(32), vbin2 varbinary(32));
INSERT INTO t VALUES
(1, x'59', x'6a'),
(2, x'5939', x'6ac3'),
(3, x'5939a998', x'6ac35d2a'),
(4, x'5939a99861154f35', x'6ac35d2a3ab34bda'),
(5, x'5939a99861154f3587d5440618e9b28b', x'6ac35d2a3ab34bda8ac412ea0141852c'),
(6, x'5939a99861154f3587d5440618e9b28b166181c5ca514ab1b8e9c970ae5e421a', x'6ac35d2a3ab34bda8ac412ea0141852c3c8e38bb19934a7092a40bb19db13a80'),
(7, x'5939a99861154f3587d5440618e9b28b', x'8ac412ea0141852c'),
(8, x'5939a99861154f35', x'6ac35d2a3ab34bda8ac412ea0141852c');

--echo #
--echo # bitwise operations with varbinary args with different sizes
--echo #

SELECT HEX(bitAnd(vbin1, vbin2)), HEX(bitOr(vbin1, vbin2)), HEX(bitXor(vbin1, vbin2)),
  HEX(bitNot(vbin1)), HEX(bitShiftLeft(vbin1, 3)), HEX(bitShiftRight(vbin2, 3)), BIT_COUNT(vbin1)
FROM t
WHERE id in(1,2,3,4,5,6);

SELECT HEX(bitAnd(vbin1, vbin2)) FROM t WHERE id=7;
SELECT HEX(bitOr(vbin1, vbin2)) FROM t WHERE id=7;
SELECT HEX(bitXor(vbin1, vbin2)) FROM t WHERE id=7;
SELECT HEX(bitShiftLeft(vbin1, 3)), HEX(bitShiftLeft(vbin2, 3)) FROM t WHERE id=7;
SELECT HEX(bitShiftRight(vbin1, 3)), HEX(bitShiftRight(vbin2, 3)) FROM t WHERE id=7;
SELECT HEX(bitNot(vbin1)), HEX(bitNot(vbin2)) FROM t WHERE id=7;
SELECT HEX(bitAnd(vbin1, vbin2)) FROM t WHERE id=8;
SELECT HEX(bitOr(vbin1, vbin2)) FROM t WHERE id=8;
SELECT HEX(bitXor(vbin1, vbin2)) FROM t WHERE id=8;
SELECT HEX(bitShiftLeft(vbin1, 3)), HEX(bitShiftLeft(vbin2, 3)) FROM t WHERE id=8;
SELECT HEX(bitShiftRight(vbin1, 3)), HEX(bitShiftRight(vbin2, 3)) FROM t WHERE id=8;
SELECT HEX(bitNot(vbin1)), HEX(bitNot(vbin2)) FROM t WHERE id=8;

--echo #
--echo # bitwise operations with varbinary args in prepared statement
--echo #

SELECT HEX(bitAnd(vbin1, vbin2)), HEX(bitOr(vbin1, vbin2)), HEX(bitXor(vbin1, vbin2)),
  HEX(bitNot(vbin1)), HEX(bitShiftLeft(vbin1, 3)), HEX(bitShiftRight(vbin2, 3)), BIT_COUNT(vbin1)
FROM t
WHERE id in(1, 2, 3, 4, 5, 6);

SELECT HEX(bitAnd(vbin1, vbin2)), HEX(bitOr(vbin1, vbin2)), HEX(bitXor(vbin1, vbin2)),
  HEX(bitNot(vbin1)), HEX(bitShiftLeft(vbin1, 3)), HEX(bitShiftRight(vbin2, 3)), BIT_COUNT(vbin1)
FROM t
WHERE id in(7);

DROP TABLE t;

drop table if exists networks;
CREATE TABLE networks (
  id int(10) unsigned NOT NULL AUTO_INCREMENT,
  start varbinary(16) NOT NULL,
  end varbinary(16) NOT NULL,
  country_code varchar(2) NOT NULL,
  country varchar(255) NOT NULL,
  PRIMARY KEY (id),
  KEY start (start),
  KEY end (end)
);

--echo #
--echo # Testing bitiwise operations on a real-life test case
--echo #

INSERT INTO networks(start, end, country_code, country) VALUES
(INET6_ATON('2c0f:fff0::'),INET6_ATON('2c0f:fff0:ffff:ffff:ffff:ffff:ffff:ffff'),'NG','Nigeria'),
(INET6_ATON('2405:1d00::'),INET6_ATON('2405:1d00:ffff:ffff:ffff:ffff:ffff:ffff'),'GR','Greenland'),
(INET6_ATON('2c0f:ffe8::'),INET6_ATON('2c0f:ffe8:ffff:ffff:ffff:ffff:ffff:ffff'),'NG','Nigeria');

SELECT id, HEX(start), HEX(end), country_code, country
FROM networks
WHERE bitAnd(INET6_ATON('2c0f:fff0:1234:5678:9101:1123::'), start = INET6_ATON('2c0f:fff0::'));

SELECT id, HEX(start), HEX(end), country_code, country
FROM networks
WHERE INET6_ATON('2c0f:ffe8:1234:5678:9101:1123::') & start = INET6_ATON('2c0f:ffe8::');

SELECT id, HEX(start), HEX(end), country_code, country
FROM networks
WHERE INET6_ATON('2c0f:fff0::') | start = INET6_ATON('2c0f:fff0::');

SELECT id, HEX(start), HEX(end), country_code, country
FROM  networks
WHERE INET6_ATON('2c0f:ffe8::') | start = INET6_ATON('2c0f:ffe8::');

SELECT id, HEX(start), HEX(end), country_code, country
FROM networks
WHERE INET6_ATON('2c0f:fff0::') ^ start = INET6_ATON('::');

SELECT id, HEX(start), HEX(end), country_code, country
FROM networks
WHERE INET6_ATON('2c0f:ffe8::') ^ start = INET6_ATON('::');

DROP TABLE networks;

--echo #
--echo # Table containing columns of MySQL types
--echo #

CREATE TABLE at(_bit bit(64),
                _tin tinyint(8),
                _boo bool,
                _sms smallint signed,
                _smu smallint unsigned,
                _mes mediumint signed,
                _meu mediumint unsigned,
                _ins int signed,
                _inu int unsigned,
                _bis bigint signed,
                _biu bigint unsigned,
                _dec decimal (5,2),
                _flo float,
                _dou double,
                _yea year,
                _jsn json,
                _chr char(12),
                _vch varchar(12),
                _bin binary(255),
                _vbn varbinary(255),
                _tbl tinyblob,
                _ttx tinytext,
                _blb blob,
                _txt text,
                _mbb mediumblob,
                _mtx mediumtext,
                _lbb longblob,
                _ltx longtext,
                _pnt point,
                _dat date default '1988-12-15',
                _dtt datetime default '2015-10-24 12:00:00',
                _smp timestamp default '2015-10-24 14:00:00',
                _tim time default' 07:08:09',
                _enu enum('a', 'b', 'c'),
                _set set('a', 'b', 'c')
                );
INSERT INTO at (
    _bit,
    _tin,
    _boo,
    _sms,
    _smu,
    _mes,
    _meu,
    _ins,
    _inu,
    _bis,
    _biu,
    _dec,
    _flo,
    _dou,
    _yea,
    _jsn,
    _chr,
    _vch,
    _bin,
    _vbn,
    _tbl,
    _ttx,
    _blb,
    _txt,
    _mbb,
    _mtx,
    _lbb,
    _ltx,
    _pnt,
    _enu,
    _set
) VALUES (
    64,
    64,
    true,
    64,
    64,
    64,
    64,
    64,
    64,
    64,
    64,
    64,
    64,
    64,
    2005,
    cast('{'a': 3}' as json),
    'abcdefghijkl',
    'abcdefghijkl',
    x'CAFEBABE000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000CAFEBABE11111111',
    x'CAFEBABE00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000011111111CAFEBABE',
    x'CAFEBABE000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000CAFE1111CAFE1111',
    'abcdefg',
    x'cafebabe',
    'abcdefg',
    x'cafebabe',
    'abcdefg',
    x'cafebabe',
    'abcdefg',
    st_geomfromtext('point(1 1)'),
    1,
    1
);

SELECT * FROM at;

--echo #
--echo # Test of bitwise aggregate functions on BINARY
--echo #

CREATE TABLE t(a varbinary(10));
INSERT INTO t VALUES(0xFF00F0F0), (0xF0F0FF00);
SELECT BIT_AND(a) FROM t;
SELECT BIT_OR(a) FROM t;
SELECT BIT_XOR(a) FROM t;
SELECT HEX(BIT_AND(a)) FROM t;
SELECT HEX(BIT_OR(a)) FROM t;
SELECT HEX(BIT_XOR(a)) FROM t;
truncate table t;

-- bitwise aggregate functions with NULL value
INSERT INTO t VALUES(NULL);
SELECT HEX(BIT_AND(a)) FROM t;
SELECT HEX(BIT_OR(a)) FROM t;
SELECT HEX(BIT_XOR(a)) FROM t;
truncate table t;

-- bitwise aggregate functions when first value is null
INSERT INTO t VALUES(NULL), (0xFF00F0F0), (0xF0F0FF00);
SELECT HEX(BIT_AND(a)) FROM t;
SELECT HEX(BIT_OR(a)) FROM t;
SELECT HEX(BIT_XOR(a)) FROM t;
truncate table t;

-- bitwise aggregate functions when last value is null
INSERT INTO t VALUES(0xFF00F0F0), (0xF0F0FF00), (NULL);
SELECT HEX(BIT_AND(a)) FROM t;
SELECT HEX(BIT_OR(a)) FROM t;
SELECT HEX(BIT_XOR(a)) FROM t;
truncate table t;

-- bitwise aggregate functions when a value in the middle of the aggregate is null
INSERT INTO t VALUES(0xFF00F0F0), (NULL), (0xF0F0FF00);
SELECT HEX(BIT_AND(a)) FROM t;
SELECT HEX(BIT_OR(a)) FROM t;
SELECT HEX(BIT_XOR(a)) FROM t;
DROP TABLE t;

--echo #
--echo # bitwise aggregate functions having arguments with different sizes
--echo #

CREATE TABLE t(group_id int, a varbinary(10));
INSERT INTO t VALUES(1, 0xFF00F0F0), (1, 0xFF00);

SELECT HEX(BIT_AND(lpad(a, 10, 0x00))) FROM t;

SELECT BIT_AND(a) FROM t;
SELECT BIT_OR(a) FROM t;
SELECT BIT_XOR(a) FROM t;

SELECT group_id, HEX(BIT_AND(a)) FROM t GROUP BY group_id;

SELECT group_id, HEX(BIT_OR(a)) FROM t GROUP BY group_id;

SELECT group_id, HEX(BIT_XOR(a)) FROM t GROUP BY group_id;

SELECT HEX(BIT_AND(a)) FROM t;

SELECT HEX(BIT_OR(a)) FROM t;

SELECT HEX(BIT_XOR(a)) FROM t;
truncate table t;

INSERT INTO t VALUES(1, 0xFF00), (1, 0xFF00F0F0);
SELECT BIT_AND(a) FROM t;
SELECT BIT_OR(a) FROM t;
SELECT BIT_XOR(a) FROM t;

SELECT group_id, HEX(BIT_AND(a)) FROM t GROUP BY group_id;

SELECT group_id, HEX(BIT_OR(a)) FROM t GROUP BY group_id;

SELECT group_id, HEX(BIT_XOR(a)) FROM t GROUP BY group_id;

SELECT HEX(BIT_AND(a)) FROM t;

SELECT HEX(BIT_OR(a)) FROM t;

SELECT HEX(BIT_XOR(a)) FROM t;

truncate table t;

--echo # check group 5 results with hex literals

SELECT
HEX(0xABCDEF & 0x123456 & 0x789123),
HEX(0xABCDEF | 0x123456 | 0x789123),
HEX(0xABCDEF ^ 0x123456 ^ 0x789123);

INSERT INTO t(group_id, a) VALUES
(1, 0x34567101ABFF00F0F0),
(1, 0x34567102ABF0F0F0F0),
(1, 0x34567103ABFF00F0F0),
(1, 0x34567104ABF0F0F0F0),
(2, NULL),
(3, 0x34567104ABF0F0F0F0),
(4, 0x34567100ABF0F0F0F0),
(4, NULL),
(4, 0x34567101ABFF00F0F0),
(5, 0xABCDEF),
(5, 0x123456),
(5, 0x789123);

--echo #
--echo # aggregate functions
--echo #

SELECT group_id, HEX(BIT_AND(a)), HEX(BIT_OR(a)), HEX(BIT_XOR(a))
FROM t
GROUP BY group_id;

SELECT group_id, HEX(BIT_AND(a)), BIT_AND(192), BIT_AND(0x303233), BIT_AND(binary 'foo')
FROM t
GROUP BY group_id;

SELECT BIT_COUNT(group_id), BIT_COUNT(a), BIT_COUNT(192), BIT_COUNT(0x303233),
  BIT_COUNT(binary 'foo'), BIT_COUNT(NULL)
FROM t;

--echo #
--echo # aggregate functions in prepared statements
--echo #

SELECT HEX(BIT_AND(a)),HEX(BIT_OR(a)),HEX(BIT_XOR(a)) FROM t WHERE group_id = 5;
SELECT group_id, HEX(BIT_AND(a)), HEX(BIT_OR(a)), HEX (BIT_XOR(a)) FROM t GROUP BY group_id;
DROP TABLE t;
