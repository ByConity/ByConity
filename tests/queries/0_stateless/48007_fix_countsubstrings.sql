set enable_optimizer_white_list=0;
SELECT countSubstringsCaseInsensitive(CAST((SELECT 'ya') AS j, 'String'), '');
SELECT countSubstringsCaseInsensitive(toString(number), '') from numbers(1);
