SELECT parse_url('https://127.0.0.1/xx/xxx.cpp', 'PATH');
SELECT parse_url('https://127.0.0.1/xx/xxx.cpp', 'HOST');
SELECT parse_url('https://127.0.0.1/xx?xxx.cpp', 'QUERY');
SELECT parse_url('https://127.0.0.1/xx#xxx.cpp', 'REF');
SELECT parse_url('https://127.0.0.1/xx/xxx.cpp', 'PROTOCOL');
SELECT parse_url('https://127.0.0.1/xx/xxx.cpp', 'FILE');
