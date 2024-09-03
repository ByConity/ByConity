-- No warnings
SHOW GLOBAL VARIABLES LIKE 'abc';

-- No warnings
SHOW SESSION VARIABLES LIKE 'abc';

-- Deprecation warning
SHOW VARIABLES WHERE Variable_name LIKE 'abc';

-- No warnings
SHOW VARIABLES LIKE 'abc';

