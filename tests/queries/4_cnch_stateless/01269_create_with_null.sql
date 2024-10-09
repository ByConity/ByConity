DROP TABLE IF EXISTS data_null;
DROP TABLE IF EXISTS set_null;
DROP TABLE IF EXISTS cannot_be_nullable;

SET data_type_default_nullable='false';

CREATE TABLE data_null (
    a INT NULL,
    b INT NOT NULL,
    c Nullable(INT),
    d INT
) engine=CnchMergeTree() ORDER BY b;


INSERT INTO data_null VALUES (NULL, 2, NULL, 4);

SELECT toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d) FROM data_null;

SHOW CREATE TABLE data_null;

CREATE TABLE data_null_error (
    a Nullable(INT) NULL,
    b INT NOT NULL,
    c Nullable(INT)
) engine=CnchMergeTree() Order by b;  --{serverError 377}


CREATE TABLE data_null_error (
    a INT NULL,
    b Nullable(INT) NOT NULL,
    c Nullable(INT)
) engine=CnchMergeTree() ORDER BY b;  --{serverError 377}

SET data_type_default_nullable='true';

CREATE TABLE set_null (
    a INT NULL,
    b INT NOT NULL,
    c Nullable(INT),
    d INT,
    f DEFAULT 1
) engine=CnchMergeTree() ORDER BY b;


INSERT INTO set_null VALUES (NULL, 2, NULL, NULL, NULL);

SELECT toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d), toTypeName(f) FROM set_null;

SHOW CREATE TABLE set_null;
SHOW CREATE TABLE set_null;

-- CE allows nullable(array)
CREATE TABLE can_be_nullable (n Int8, a Array(UInt8), c Int8 NOT NULL) ENGINE=CnchMergeTree ORDER BY c;
DROP TABLE can_be_nullable;

CREATE TABLE cannot_be_nullable (n Int8, a Array(UInt8) NOT NULL, c Int8 NOT NULL) ENGINE=CnchMergeTree ORDER BY c;
SHOW CREATE TABLE cannot_be_nullable;
SHOW CREATE TABLE cannot_be_nullable;

DROP TABLE data_null;
DROP TABLE set_null;
DROP TABLE cannot_be_nullable;

