DROP TABLE IF EXISTS tbl;
DROP TABLE IF EXISTS tbl2;

CREATE TABLE tbl
(
    `start_dt` DateTime64(0),
    `end_dt` DateTime64(0)
)
ENGINE = CnchMergeTree()
ORDER BY start_dt
UNIQUE KEY (start_dt, end_dt);

INSERT INTO tbl VALUES ('1969-03-28 17:00:00', '1969-03-29 18:00:00'), ('1970-03-28 17:00:00', '1970-03-29 18:00:00');

SELECT * FROM tbl ORDER BY start_dt, end_dt;

CREATE TABLE tbl2
(
    `point_x` Decimal64(2),
    `point_y` Decimal64(2)
)
ENGINE = CnchMergeTree()
ORDER BY point_x
UNIQUE KEY (point_x, point_y);

INSERT INTO tbl2 (point_x, point_y) VALUES('-22.22', '-23.33'), ('17.77', '18.88');

SELECT * FROM tbl2 ORDER BY point_x, point_y;

DROP TABLE IF EXISTS tbl;
DROP TABLE IF EXISTS tbl2;
