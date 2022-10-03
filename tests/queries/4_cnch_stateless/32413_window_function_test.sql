

DROP TABLE IF EXISTS ppl4;
CREATE TABLE ppl4 (id Int, department String, onboard_date String, age Int) ENGINE = CnchMergeTree() ORDER BY tuple();

INSERT INTO ppl4 VALUES('1', 'data', '2019-01-01', '20');
INSERT INTO ppl4 VALUES('2', 'data', '2019-03-01', '21');
INSERT INTO ppl4 VALUES('3', 'data', '2019-02-01', '22');
INSERT INTO ppl4 VALUES('4', 'data', '2019-03-01', '23');
INSERT INTO ppl4 VALUES('5', 'data', '2019-04-01', '24');
INSERT INTO ppl4 VALUES('6', 'payment', '2019-01-01', '25');
INSERT INTO ppl4 VALUES('7', 'payment', '2019-02-01', '26');
INSERT INTO ppl4 VALUES('8', 'payment', '2019-04-01', '27');
INSERT INTO ppl4 VALUES('9', 'payment', '2019-05-01', '28');
INSERT INTO ppl4 VALUES('10', 'solution', '2019-08-01', '29');
INSERT INTO ppl4 VALUES('11', 'solution', '2019-08-01', '30');
INSERT INTO ppl4 VALUES('12', 'solution', '2019-09-01', '31');
INSERT INTO ppl4 VALUES('13', 'ML', '2019-03-01', '32');
INSERT INTO ppl4 VALUES('14', 'ML', '2019-12-01', '33');
INSERT INTO ppl4 VALUES('15', 'ML', '2019-03-01', '34');
INSERT INTO ppl4 VALUES('16', 'ML', '2019-02-01', '35');


SELECT
    row_number() OVER (ORDER BY age ASC)
FROM ppl4 
Order BY id;


DROP TABLE ppl4;
