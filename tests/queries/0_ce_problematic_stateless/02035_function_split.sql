SELECT split(x, '\\d+') FROM (SELECT arrayJoin(['a1ba5ba8b', 'a11ba5689ba891011b']) x);
SELECT split('abcde', '');
SELECT split(x, '<[^<>]*>') FROM (SELECT arrayJoin(['<h1>hello<h2>world</h2></h1>', 'gbye<split>bug']) x);
SELECT split('', 'ab');
SELECT split('', '');
SELECT split('127.0.0.1', './/');
SELECT split('127.0.0.1', '.');
