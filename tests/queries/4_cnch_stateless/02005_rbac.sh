#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e

export NEW_USER=${NEW_USER:="user_test_02184"}
export NEW_USER1=${NEW_USER1:="user_test_02185"}

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS user_test_02184;"
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS user_test_02185;"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t;"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS int_table;"
$CLICKHOUSE_CLIENT --query "DROP ROLE IF EXISTS role1;"
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS \`1234\`;"

$CLICKHOUSE_CLIENT --query "CREATE USER user_test_02184 IDENTIFIED WITH plaintext_password BY 'user_test_02184';"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON *.* FROM user_test_02184;"

$CLICKHOUSE_CLIENT --query "GRANT CREATE ON *.* TO user_test_02184;"
[ -v TENANT_ID ] && NEW_USER="${TENANT_ID}\`user_test_02184"
$CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS int_table (a int) engine = CnchMergeTree ORDER BY a;"
$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "SELECT * FROM int_table;" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "SELECT * FROM int_table settings enable_optimizer=1;" 2>&1| grep -Fo "Not enough privileges" | uniq

$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "CREATE TABLE t AS int_table;"
$CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE t;"
$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "DROP TABLE t;" 2>&1| grep -Fo "Not enough privileges" | uniq

$CLICKHOUSE_CLIENT --query "CREATE ROLE role1;"
$CLICKHOUSE_CLIENT --query "GRANT DROP ON *.* TO role1;"
$CLICKHOUSE_CLIENT --query "GRANT role1 to user_test_02184;"
$CLICKHOUSE_CLIENT --query "ALTER USER user_test_02184 DEFAULT ROLE role1;"
$CLICKHOUSE_CLIENT --query "SHOW GRANTS FOR user_test_02184 FORMAT CSV;"
$CLICKHOUSE_CLIENT --query "SELECT * from system.role_grants FORMAT CSV;"

# In next login, default should apply to user_test_02184 and he can drop table
$CLICKHOUSE_CLIENT --user=$NEW_USER --password=user_test_02184  --query "DROP TABLE t;"

# Create new user user_test_02185 and user_test_02184 will grant SELECT permissions to user_test_02185
$CLICKHOUSE_CLIENT --query "CREATE USER user_test_02185 IDENTIFIED WITH plaintext_password BY 'user_test_02185';"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON int_table TO user_test_02184 WITH GRANT OPTION;"
$CLICKHOUSE_CLIENT --query "INSERT INTO int_table VALUES (1);"
$CLICKHOUSE_CLIENT --user=$NEW_USER --password=user_test_02184 --query "GRANT SELECT ON int_table TO user_test_02185;"
[ -v TENANT_ID ] && NEW_USER1="${TENANT_ID}\`user_test_02185"
$CLICKHOUSE_CLIENT --user=$NEW_USER1 --password=user_test_02185 --query "SELECT a FROM int_table;"
$CLICKHOUSE_CLIENT --user=$NEW_USER1 --password=user_test_02185 --query "SELECT a FROM int_table settings enable_optimizer=1;"

# These set of GRANTs should not result in putCAS error
$CLICKHOUSE_CLIENT --query "CREATE USER \`1234\` IDENTIFIED WITH plaintext_password BY 'user_test_02184';"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_beorn_s_hall_34567ff0\`.e2e_country_round_d229fe1c TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_caraxes_bdc2627f\`.e2e_azkaban_35396707 TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SHOW DATABASES, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP DATABASE, DROP TABLE, DROP VIEW, DROP DICTIONARY ON \`1.e2e_carc_73739486\`.* TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_carc_73739486\`.e2e_bilbo_baggins_aef88fe6 TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_galladon_of_tarth_8595e483\`.e2e_under_hill_e5af847b TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_hallyne_6d4eed77\`.e2e_bronn_92d17a4d TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SHOW DATABASES, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP DATABASE, DROP TABLE, DROP VIEW, DROP DICTIONARY ON \`1.e2e_mafalda_hopkirk_39da3d1a\`.* TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_mafalda_hopkirk_39da3d1a\`.e2e_bartemius_crouch___df5928a8 TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_mount_gundabad_38907ad2\`.e2e_severus_snape_882b290c TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_serwyn_of_the_mirr_5208d575\`.e2e_alastor__mad_eye___2632e317 TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_shrykos_0bbb8bd8\`.e2e_bungo_baggins_76667cfb TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_syrax_b95bcf97\`.e2e_viserion_798f4304 TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_torwold_browntooth_8764ef9a\`.e2e_shrykos_36b6717b TO \`1234\` WITH GRANT OPTION"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON \`1.e2e_vhagar_f506e4fa\`.e2e_dean_thomas_0990f1c8 TO \`1234\` WITH GRANT OPTION"


$CLICKHOUSE_CLIENT --query "DROP TABLE int_table;"
$CLICKHOUSE_CLIENT --query "DROP USER user_test_02184;"
$CLICKHOUSE_CLIENT --query "DROP USER user_test_02185;"
$CLICKHOUSE_CLIENT --query "DROP ROLE role1;"
$CLICKHOUSE_CLIENT --query "DROP USER \`1234\`;"
