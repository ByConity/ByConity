SELECT FIELD('aa', 'bb', 'aa', 'cc');
SELECT FIELD('2000', 2000, '2000');
SELECT FIELD(2000, 2005, '2000');
SELECT FIELD(number, 'Aa', 'Bb', 'Cc', number) FROM (SELECT * FROM numbers(10));
SELECT FIELD('cc', 'Aa', 'Bb', 'Cc', number) FROM (SELECT * FROM numbers(10));
SELECT FIELD('1', 'Aa', 'Bb', 'Cc', number) FROM (SELECT * FROM numbers(10));
