SELECT arrayJoin(toNullable(array(1, 2, 3)));
SELECT arrayJoin(toNullable(array(NULL)));
SELECT arrayJoin(toNullable(1)); -- { serverError 53 }
SELECT arrayJoin(toNullable(array(tuple(1, 'a'), tuple(2, 'b'))));
