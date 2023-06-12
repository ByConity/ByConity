SELECT FIND_IN_SET('a','a,b,c,d');
SELECT FIND_IN_SET('aa','a,b,c,aa');
SELECT FIND_IN_SET(200,'a,200,c,d');
SELECT FIND_IN_SET('a','aa,b,c,d');
SELECT FIND_IN_SET(NULL,'a,b,c,d');
SELECT FIND_IN_SET('a',NULL);