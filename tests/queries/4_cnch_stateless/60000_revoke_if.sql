-- Create a test user
CREATE USER IF NOT EXISTS test_user IDENTIFIED BY 'password';

-- Grant all privileges on all databases and tables to the test user
GRANT ALL ON *.* TO test_user;
GRANT SELECT ON db.tbl TO test_user;

-- Revoke SELECT on a specific database and table only exactly matches
REVOKE IF EXISTS SELECT ON db.tbl FROM test_user;

-- Show the grants for the test user again
SHOW GRANTS FOR test_user;
SELECT * FROM system.sensitive_grants where user_name like '%test_user' FORMAT CSV;

-- Clean up - drop the test user after the test
DROP USER IF EXISTS test_user;
