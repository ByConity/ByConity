USE test;
DROP TABLE IF EXISTS constraint_skip_violate;
CREATE TABLE constraint_skip_violate
(
    id UInt64,
    CONSTRAINT `c0` CHECK 1,
    CONSTRAINT `c1` CHECK id < 10,
    CONSTRAINT `c2` CHECK id > 5
) ENGINE = TinyLog();

SET constraint_skip_violate = 1;

INSERT INTO constraint_skip_violate VALUES (1);

SELECT * FROM constraint_skip_violate;

INSERT INTO constraint_skip_violate VALUES (5), (10), (6), (4), (7);

SELECT * FROM constraint_skip_violate;

DROP TABLE constraint_skip_violate;

CREATE TABLE constraint_skip_violate
(
    id Nullable(UInt64),
    CONSTRAINT `c0` CHECK id < 10,
    CONSTRAINT `c1` CHECK id > 5
) ENGINE = TinyLog();

INSERT INTO constraint_skip_violate VALUES (Null);

SELECT * FROM constraint_skip_violate;

INSERT INTO constraint_skip_violate VALUES (5), (10), (6), (4), (7);

SELECT * FROM constraint_skip_violate;

DROP TABLE constraint_skip_violate;
