DROP USER IF EXISTS test_user_01075;
CREATE USER test_user_01075;

GRANT SHOW ON test.* TO test_user_01075;
REVOKE SHOW ON *.* FROM test_user_01075;

GRANT SHOW ON test.table_1 TO test_user_01075;
REVOKE SHOW ON *.* FROM test_user_01075;

GRANT SHOW ON test.* TO test_user_01075;
SHOW GRANTS FOR test_user_01075;
REVOKE SHOW ON *.* FROM test_user_01075;

SHOW GRANTS FOR test_user_01075;
DROP USER test_user_01075;
