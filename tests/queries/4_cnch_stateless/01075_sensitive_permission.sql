DROP USER IF EXISTS test_user_01074;
CREATE USER test_user_01074;

GRANT SHOW ON test.* TO test_user_01074;
REVOKE SHOW ON *.* FROM test_user_01074;

GRANT SHOW ON test.table_1 TO test_user_01074;
REVOKE SHOW ON *.* FROM test_user_01074;

GRANT SHOW ON test.* TO test_user_01074;
SHOW GRANTS FOR test_user_01074;
REVOKE SHOW ON *.* FROM test_user_01074;

SHOW GRANTS FOR test_user_01074;
DROP USER test_user_01074;
