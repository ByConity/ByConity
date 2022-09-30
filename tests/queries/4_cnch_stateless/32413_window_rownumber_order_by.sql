


DROP TABLE IF EXISTS rownumber_order_by;
CREATE TABLE rownumber_order_by (id Int, department String, onboard_date String, age Int) ENGINE = CnchMergeTree() ORDER BY tuple();

INSERT INTO rownumber_order_by VALUES('1', 'data', '2019-01-01', '20');
INSERT INTO rownumber_order_by VALUES('2', 'data', '2019-03-01', '21');
INSERT INTO rownumber_order_by VALUES('3', 'data', '2019-02-01', '22');
INSERT INTO rownumber_order_by VALUES('4', 'data', '2019-03-01', '23');
INSERT INTO rownumber_order_by VALUES('5', 'data', '2019-04-01', '24');
INSERT INTO rownumber_order_by VALUES('6', 'payment', '2019-01-01', '25');
INSERT INTO rownumber_order_by VALUES('7', 'payment', '2019-02-01', '26');
INSERT INTO rownumber_order_by VALUES('8', 'payment', '2019-04-01', '27');
INSERT INTO rownumber_order_by VALUES('9', 'payment', '2019-05-01', '28');
INSERT INTO rownumber_order_by VALUES('10', 'solution', '2019-08-01', '29');
INSERT INTO rownumber_order_by VALUES('11', 'solution', '2019-08-01', '30');
INSERT INTO rownumber_order_by VALUES('12', 'solution', '2019-09-01', '31');
INSERT INTO rownumber_order_by VALUES('13', 'ML', '2019-03-01', '32');
INSERT INTO rownumber_order_by VALUES('14', 'ML', '2019-12-01', '33');
INSERT INTO rownumber_order_by VALUES('15', 'ML', '2019-03-01', '34');
INSERT INTO rownumber_order_by VALUES('16', 'ML', '2019-02-01', '35');


SELECT
    row_number() OVER (ORDER BY age ASC)
FROM rownumber_order_by 
Order BY id;


DROP TABLE rownumber_order_by;
