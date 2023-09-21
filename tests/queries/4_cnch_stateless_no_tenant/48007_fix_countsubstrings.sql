SELECT countSubstringsCaseInsensitive(CAST((SELECT 'ya') AS j, 'String'), '');
SELECT countSubstringsCaseInsensitive(toString(number), '') from numbers(1);
