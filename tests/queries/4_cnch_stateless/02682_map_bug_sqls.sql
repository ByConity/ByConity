-- Type checks in comparision. If using const string, will try to deserialize to corrosponding types
SELECT map('a', 'b') = ''; -- { serverError 27 }
SELECT map('a', 'b') = '{\'a\': \'b\'}';
SELECT map('a', 'b') = materialize('{\'a\': \'b\'}'); -- { serverError 386 }
