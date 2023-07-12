-- week mode [0,7],  week test case. refer to the mysql test case
SELECT toWeek('1998-01-01'), toWeek('1997-01-01'), toWeek('1998-01-01', 1), toWeek('1997-01-01', 1);
SELECT toWeek('1998-12-31'), toWeek('1997-12-31'), toWeek('1998-12-31', 1), toWeek('1997-12-31', 1);
SELECT toWeek('1995-01-01'), toWeek('1995-01-01', 1);
SELECT toYearWeek('1981-12-31', 1), toYearWeek('1982-01-01', 1), toYearWeek('1982-12-31', 1), toYearWeek('1983-01-01', 1);
SELECT toYearWeek('1987-01-01', 1), toYearWeek('1987-01-01');	
	
SELECT toWeek('2000-01-01',0) AS w2000, toWeek('2001-01-01',0) AS w2001, toWeek('2002-01-01',0) AS w2002,toWeek('2003-01-01',0) AS w2003, toWeek('2004-01-01',0) AS w2004, toWeek('2005-01-01',0) AS w2005, toWeek('2006-01-01',0) AS w2006;
SELECT toWeek('2000-01-06',0) AS w2000, toWeek('2001-01-06',0) AS w2001, toWeek('2002-01-06',0) AS w2002,toWeek('2003-01-06',0) AS w2003, toWeek('2004-01-06',0) AS w2004, toWeek('2005-01-06',0) AS w2005, toWeek('2006-01-06',0) AS w2006;
SELECT toWeek('2000-01-01',1) AS w2000, toWeek('2001-01-01',1) AS w2001, toWeek('2002-01-01',1) AS w2002,toWeek('2003-01-01',1) AS w2003, toWeek('2004-01-01',1) AS w2004, toWeek('2005-01-01',1) AS w2005, toWeek('2006-01-01',1) AS w2006;
SELECT toWeek('2000-01-06',1) AS w2000, toWeek('2001-01-06',1) AS w2001, toWeek('2002-01-06',1) AS w2002,toWeek('2003-01-06',1) AS w2003, toWeek('2004-01-06',1) AS w2004, toWeek('2005-01-06',1) AS w2005, toWeek('2006-01-06',1) AS w2006;
SELECT toYearWeek('2000-01-01',0) AS w2000, toYearWeek('2001-01-01',0) AS w2001, toYearWeek('2002-01-01',0) AS w2002,toYearWeek('2003-01-01',0) AS w2003, toYearWeek('2004-01-01',0) AS w2004, toYearWeek('2005-01-01',0) AS w2005, toYearWeek('2006-01-01',0) AS w2006;
SELECT toYearWeek('2000-01-06',0) AS w2000, toYearWeek('2001-01-06',0) AS w2001, toYearWeek('2002-01-06',0) AS w2002,toYearWeek('2003-01-06',0) AS w2003, toYearWeek('2004-01-06',0) AS w2004, toYearWeek('2005-01-06',0) AS w2005, toYearWeek('2006-01-06',0) AS w2006;
SELECT toYearWeek('2000-01-01',1) AS w2000, toYearWeek('2001-01-01',1) AS w2001, toYearWeek('2002-01-01',1) AS w2002,toYearWeek('2003-01-01',1) AS w2003, toYearWeek('2004-01-01',1) AS w2004, toYearWeek('2005-01-01',1) AS w2005, toYearWeek('2006-01-01',1) AS w2006;
SELECT toYearWeek('2000-01-06',1) AS w2000, toYearWeek('2001-01-06',1) AS w2001, toYearWeek('2002-01-06',1) AS w2002,toYearWeek('2003-01-06',1) AS w2003, toYearWeek('2004-01-06',1) AS w2004, toYearWeek('2005-01-06',1) AS w2005, toYearWeek('2006-01-06',1) AS w2006;	
SELECT toWeek('1998-12-31',2),toWeek('1998-12-31',3), toWeek('2000-01-01',2), toWeek('2000-01-01',3);
SELECT toWeek('2000-12-31',2),toWeek('2000-12-31',3);

SELECT toWeek('1998-12-31',0) AS w0, toWeek('1998-12-31',1) AS w1, toWeek('1998-12-31',2) AS w2, toWeek('1998-12-31',3) AS w3, toWeek('1998-12-31',4) AS w4, toWeek('1998-12-31',5) AS w5, toWeek('1998-12-31',6) AS w6, toWeek('1998-12-31',7) AS w7;
SELECT toWeek('2000-01-01',0) AS w0, toWeek('2000-01-01',1) AS w1, toWeek('2000-01-01',2) AS w2, toWeek('2000-01-01',3) AS w3, toWeek('2000-01-01',4) AS w4, toWeek('2000-01-01',5) AS w5, toWeek('2000-01-01',6) AS w6, toWeek('2000-01-01',7) AS w7;
SELECT toWeek('2000-01-06',0) AS w0, toWeek('2000-01-06',1) AS w1, toWeek('2000-01-06',2) AS w2, toWeek('2000-01-06',3) AS w3, toWeek('2000-01-06',4) AS w4, toWeek('2000-01-06',5) AS w5, toWeek('2000-01-06',6) AS w6, toWeek('2000-01-06',7) AS w7;
SELECT toWeek('2000-12-31',0) AS w0, toWeek('2000-12-31',1) AS w1, toWeek('2000-12-31',2) AS w2, toWeek('2000-12-31',3) AS w3, toWeek('2000-12-31',4) AS w4, toWeek('2000-12-31',5) AS w5, toWeek('2000-12-31',6) AS w6, toWeek('2000-12-31',7) AS w7;
SELECT toWeek('2001-01-01',0) AS w0, toWeek('2001-01-01',1) AS w1, toWeek('2001-01-01',2) AS w2, toWeek('2001-01-01',3) AS w3, toWeek('2001-01-01',4) AS w4, toWeek('2001-01-01',5) AS w5, toWeek('2001-01-01',6) AS w6, toWeek('2001-01-01',7) AS w7;

SELECT toYearWeek('2000-12-31',0), toYearWeek('2000-12-31',1), toYearWeek('2000-12-31',2), toYearWeek('2000-12-31',3), toYearWeek('2000-12-31',4), toYearWeek('2000-12-31',5), toYearWeek('2000-12-31',6), toYearWeek('2000-12-31',7);