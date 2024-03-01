#include <Databases/DatabaseOnDisk.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/System/attachSystemTablesImpl.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

/// Below are SQL definitions for views in "information_schema". Perhaps it would be more aesthetic to have them in .sql files
/// and embed them here instead. In fact, it has been that way using INCBIN macros until #54773. The problem was that when
/// existing .sql files were changed, the build system did not recognize that this source (.cpp) file changed and instead used
/// cached object files from previous builds.
///
/// INCBIN is one of many libraries to embed external data. We might wait a little bit longer and try #embed (*) which should
/// solve the problem once and for all after 40 years.
///
/// (*) https://thephd.dev/finally-embed-in-c23

static constexpr std::string_view schemata = R"(
    ATTACH VIEW schemata
    (
        `catalog_name` String,
        `schema_name` String,
        `schema_owner` String,
        `default_character_set_catalog` Nullable(String),
        `default_character_set_schema` Nullable(String),
        `default_character_set_name` Nullable(String),
        `sql_path` Nullable(String),
        `CATALOG_NAME` String,
        `SCHEMA_NAME` String,
        `SCHEMA_OWNER` String,
        `DEFAULT_CHARACTER_SET_CATALOG` Nullable(String),
        `DEFAULT_CHARACTER_SET_SCHEMA` Nullable(String),
        `DEFAULT_CHARACTER_SET_NAME` Nullable(String),
        `SQL_PATH` Nullable(String)
    ) AS
    SELECT
        name                          AS catalog_name,
        name                          AS schema_name,
        'default'                     AS schema_owner,
        NULL                          AS default_character_set_catalog,
        NULL                          AS default_character_set_schema,
        NULL                          AS default_character_set_name,
        NULL                          AS sql_path,
        catalog_name                  AS CATALOG_NAME,
        schema_name                   AS SCHEMA_NAME,
        schema_owner                  AS SCHEMA_OWNER,
        default_character_set_catalog AS DEFAULT_CHARACTER_SET_CATALOG,
        default_character_set_schema  AS DEFAULT_CHARACTER_SET_SCHEMA,
        default_character_set_name    AS DEFAULT_CHARACTER_SET_NAME,
        sql_path                      AS SQL_PATH
    FROM system.databases
)";

static constexpr std::string_view tables = R"(
    ATTACH VIEW tables
    (
        `table_catalog` String,
        `table_schema` String,
        `table_name` String,
        `table_type` String,
        `data_length` Nullable(UInt64),
        `index_length` Nullable(UInt64),
        `table_collation` Nullable(String),
        `table_comment` Nullable(String),
        `TABLE_CATALOG` String,
        `TABLE_SCHEMA` String,
        `TABLE_NAME` String,
        `TABLE_TYPE` String,
        `DATA_LENGTH` Nullable(UInt64),
        `INDEX_LENGTH` Nullable(UInt64),
        `TABLE_COLLATION` Nullable(String),
        `TABLE_COMMENT` Nullable(String)
    ) AS
    SELECT
        database             AS table_catalog,
        database             AS table_schema,
        name                 AS table_name,
        multiIf(is_temporary,          'LOCAL TEMPORARY',
                engine LIKE '%View',   'VIEW',
                engine LIKE 'System%', 'SYSTEM VIEW',
                'TABLE'
                )            AS table_type,
        total_bytes AS data_length,
        0                    AS index_length,
        'utf8mb4_0900_ai_ci' AS table_collation,
        comment              AS table_comment,
        table_catalog        AS TABLE_CATALOG,
        table_schema         AS TABLE_SCHEMA,
        table_name           AS TABLE_NAME,
        table_type           AS TABLE_TYPE,
        data_length          AS DATA_LENGTH,
        index_length         AS INDEX_LENGTH,
        table_collation      AS TABLE_COLLATION,
        table_comment        AS TABLE_COMMENT
    FROM system.tables
)";

static constexpr std::string_view views = R"(
    ATTACH VIEW views
    (
        `table_catalog` String,
        `table_schema` String,
        `table_name` String,
        `view_definition` String,
        `check_option` String,
        `is_updatable` Enum8('NO' = 0, 'YES' = 1),
        `is_insertable_into` Enum8('NO' = 0, 'YES' = 1),
        `is_trigger_updatable` Enum8('NO' = 0, 'YES' = 1),
        `is_trigger_deletable` Enum8('NO' = 0, 'YES' = 1),
        `is_trigger_insertable_into` Enum8('NO' = 0, 'YES' = 1),
        `TABLE_CATALOG` String,
        `TABLE_SCHEMA` String,
        `TABLE_NAME` String,
        `VIEW_DEFINITION` String,
        `CHECK_OPTION` String,
        `DEFINER` String,
        `IS_UPDATABLE` Enum8('NO' = 0, 'YES' = 1),
        `IS_INSERTABLE_INTO` Enum8('NO' = 0, 'YES' = 1),
        `IS_TRIGGER_UPDATABLE` Enum8('NO' = 0, 'YES' = 1),
        `IS_TRIGGER_DELETABLE` Enum8('NO' = 0, 'YES' = 1),
        `IS_TRIGGER_INSERTABLE_INTO` Enum8('NO' = 0, 'YES' = 1)
    ) AS
    SELECT
        database AS table_catalog,
        database AS table_schema,
        name AS table_name,
        as_select AS view_definition,
        'NONE' AS check_option,
        0 AS is_updatable,
        engine = 'MaterializedView' AS is_insertable_into,
        0 AS is_trigger_updatable,
        0 AS is_trigger_deletable,
        0 AS is_trigger_insertable_into,
        table_catalog AS TABLE_CATALOG,
        table_schema AS TABLE_SCHEMA,
        table_name AS TABLE_NAME,
        view_definition AS VIEW_DEFINITION,
        check_option AS CHECK_OPTION,
        '' AS DEFINER,
        is_updatable AS IS_UPDATABLE,
        is_insertable_into AS IS_INSERTABLE_INTO,
        is_trigger_updatable AS IS_TRIGGER_UPDATABLE,
        is_trigger_deletable AS IS_TRIGGER_DELETABLE,
        is_trigger_insertable_into AS IS_TRIGGER_INSERTABLE_INTO
    FROM system.tables
    WHERE engine LIKE '%View'
)";

static constexpr std::string_view columns = R"(
    ATTACH VIEW columns
    (
        `table_catalog` String,
        `table_schema` String,
        `table_name` String,
        `column_name` String,
        `ordinal_position` UInt64,
        `column_default` String,
        `is_nullable` String,
        `data_type` String,
        `character_maximum_length` Nullable(UInt64),
        `character_octet_length` Nullable(UInt64),
        `numeric_precision` Nullable(UInt64),
        `numeric_precision_radix` Nullable(UInt64),
        `numeric_scale` Nullable(UInt64),
        `datetime_precision` Nullable(UInt64),
        `character_set_catalog` Nullable(String),
        `character_set_schema` Nullable(String),
        `character_set_name` Nullable(String),
        `collation_catalog` Nullable(String),
        `collation_schema` Nullable(String),
        `collation_name` Nullable(String),
        `domain_catalog` Nullable(String),
        `domain_schema` Nullable(String),
        `domain_name` Nullable(String),
        `column_comment` String,
        `column_type` String,
        `TABLE_CATALOG` String,
        `TABLE_SCHEMA` String,
        `TABLE_NAME` String,
        `COLUMN_NAME` String,
        `ORDINAL_POSITION` UInt64,
        `COLUMN_DEFAULT` String,
        `IS_NULLABLE` String,
        `DATA_TYPE` String,
        `CHARACTER_MAXIMUM_LENGTH` Nullable(UInt64),
        `CHARACTER_OCTET_LENGTH` Nullable(UInt64),
        `NUMERIC_PRECISION` Nullable(UInt64),
        `NUMERIC_PRECISION_RADIX` Nullable(UInt64),
        `NUMERIC_SCALE` Nullable(UInt64),
        `DATETIME_PRECISION` Nullable(UInt64),
        `CHARACTER_SET_CATALOG` Nullable(String),
        `CHARACTER_SET_SCHEMA` Nullable(String),
        `CHARACTER_SET_NAME` Nullable(String),
        `COLLATION_CATALOG` Nullable(String),
        `COLLATION_SCHEMA` Nullable(String),
        `COLLATION_NAME` Nullable(String),
        `DOMAIN_CATALOG` Nullable(String),
        `DOMAIN_SCHEMA` Nullable(String),
        `DOMAIN_NAME` Nullable(String),
        `COLUMN_COMMENT` String,
        `COLUMN_TYPE` String,
        `EXTRA` String
    ) AS
    SELECT
        database AS table_catalog,
        database AS table_schema,
        table AS table_name,
        name AS column_name,
        position AS ordinal_position,
        default_expression AS column_default,
        type LIKE 'Nullable(%)' AS is_nullable,
        convertToDialectDataType(type) AS data_type,
        character_octet_length AS character_maximum_length,
        character_octet_length,
        numeric_precision,
        numeric_precision_radix,
        numeric_scale,
        datetime_precision,
        NULL AS character_set_catalog,
        NULL AS character_set_schema,
        NULL AS character_set_name,
        NULL AS collation_catalog,
        NULL AS collation_schema,
        NULL AS collation_name,
        NULL AS domain_catalog,
        NULL AS domain_schema,
        NULL AS domain_name,
        comment AS column_comment,
        convertToDialectColumnType(type) AS column_type,
        table_catalog AS TABLE_CATALOG,
        table_schema AS TABLE_SCHEMA,
        table_name AS TABLE_NAME,
        column_name AS COLUMN_NAME,
        ordinal_position AS ORDINAL_POSITION,
        column_default AS COLUMN_DEFAULT,
        is_nullable AS IS_NULLABLE,
        data_type AS DATA_TYPE,
        character_maximum_length AS CHARACTER_MAXIMUM_LENGTH,
        character_octet_length AS CHARACTER_OCTET_LENGTH,
        numeric_precision AS NUMERIC_PRECISION,
        numeric_precision_radix AS NUMERIC_PRECISION_RADIX,
        numeric_scale AS NUMERIC_SCALE,
        datetime_precision AS DATETIME_PRECISION,
        character_set_catalog AS CHARACTER_SET_CATALOG,
        character_set_schema AS CHARACTER_SET_SCHEMA,
        character_set_name AS CHARACTER_SET_NAME,
        collation_catalog AS COLLATION_CATALOG,
        collation_schema AS COLLATION_SCHEMA,
        collation_name AS COLLATION_NAME,
        domain_catalog AS DOMAIN_CATALOG,
        domain_schema AS DOMAIN_SCHEMA,
        domain_name AS DOMAIN_NAME,
        column_comment AS COLUMN_COMMENT,
        column_type AS COLUMN_TYPE,
        '' AS EXTRA
    FROM system.columns
)";

static constexpr std::string_view key_column_usage = R"(
    ATTACH VIEW key_column_usage
        (
         `constraint_catalog` String,
         `constraint_schema` String,
         `constraint_name` Nullable(String),
         `table_catalog` String,
         `table_schema` String,
         `table_name` String,
         `column_name` Nullable(String),
         `ordinal_position` UInt32,
         `position_in_unique_constraint` Nullable(UInt32),
         `referenced_table_schema` Nullable(String),
         `referenced_table_name` Nullable(String),
         `referenced_column_name` Nullable(String),
         `CONSTRAINT_CATALOG` Nullable(String),
         `CONSTRAINT_SCHEMA` Nullable(String),
         `CONSTRAINT_NAME` Nullable(String),
         `TABLE_CATALOG` String,
         `TABLE_SCHEMA` String,
         `TABLE_NAME` String,
         `COLUMN_NAME` Nullable(String),
         `ORDINAL_POSITION` UInt32,
         `POSITION_IN_UNIQUE_CONSTRAINT` Nullable(UInt32),
         `REFERENCED_TABLE_SCHEMA` Nullable(String),
         `REFERENCED_TABLE_NAME` Nullable(String),
         `REFERENCED_COLUMN_NAME` Nullable(String)
    ) AS
    SELECT
        'def'                         AS constraint_catalog,
        database                      AS constraint_schema,
        'PRIMARY'                     AS constraint_name,
        'def'                         AS table_catalog,
        database                      AS table_schema,
        table                         AS table_name,
        name                          AS column_name,
        1                             AS ordinal_position,
        NULL                          AS position_in_unique_constraint,
        NULL                          AS referenced_table_schema,
        NULL                          AS referenced_table_name,
        NULL                          AS referenced_column_name,
        constraint_catalog            AS CONSTRAINT_CATALOG,
        constraint_schema             AS CONSTRAINT_SCHEMA,
        constraint_name               AS CONSTRAINT_NAME,
        table_catalog                 AS TABLE_CATALOG,
        table_schema                  AS TABLE_SCHEMA,
        table_name                    AS TABLE_NAME,
        column_name                   AS COLUMN_NAME,
        ordinal_position              AS ORDINAL_POSITION,
        position_in_unique_constraint AS POSITION_IN_UNIQUE_CONSTRAINT,
        referenced_table_schema       AS REFERENCED_TABLE_SCHEMA,
        referenced_table_name         AS REFERENCED_TABLE_NAME,
        referenced_column_name        AS REFERENCED_COLUMN_NAME
    FROM system.columns
    WHERE is_in_primary_key;
)";

static constexpr std::string_view referential_constraints = R"(
    ATTACH VIEW referential_constraints
        (
         `constraint_catalog` String,
         `constraint_schema` String,
         `constraint_name` Nullable(String),
         `unique_constraint_catalog` String,
         `unique_constraint_schema` String,
         `unique_constraint_name` Nullable(String),
         `match_option` String,
         `update_rule` String,
         `delete_rule` String,
         `table_name` String,
         `referenced_table_name` String,
         `CONSTRAINT_CATALOG` String,
         `CONSTRAINT_SCHEMA` String,
         `CONSTRAINT_NAME` Nullable(String),
         `UNIQUE_CONSTRAINT_CATALOG` String,
         `UNIQUE_CONSTRAINT_SCHEMA` String,
         `UNIQUE_CONSTRAINT_NAME` Nullable(String),
         `MATCH_OPTION` String,
         `UPDATE_RULE` String,
         `DELETE_RULE` String,
         `TABLE_NAME` String,
         `REFERENCED_TABLE_NAME` String
    ) AS
    SELECT
        ''                        AS constraint_catalog,
        NULL                      AS constraint_name,
        ''                        AS constraint_schema,
        ''                        AS unique_constraint_catalog,
        NULL                      AS unique_constraint_name,
        ''                        AS unique_constraint_schema,
        ''                        AS match_option,
        ''                        AS update_rule,
        ''                        AS delete_rule,
        ''                        AS table_name,
        ''                        AS referenced_table_name,
        constraint_catalog        AS CONSTRAINT_CATALOG,
        constraint_name           AS CONSTRAINT_NAME,
        constraint_schema         AS CONSTRAINT_SCHEMA,
        unique_constraint_catalog AS UNIQUE_CONSTRAINT_CATALOG,
        unique_constraint_name    AS UNIQUE_CONSTRAINT_NAME,
        unique_constraint_schema  AS UNIQUE_CONSTRAINT_SCHEMA,
        match_option              AS MATCH_OPTION,
        update_rule               AS UPDATE_RULE,
        delete_rule               AS DELETE_RULE,
        table_name                AS TABLE_NAME,
        referenced_table_name     AS REFERENCED_TABLE_NAME
    WHERE false; -- make sure this view is always empty
)";

static constexpr std::string_view statistics = R"(
    ATTACH VIEW statistics
        (
        `table_catalog` String,
        `table_schema` String,
        `table_name` String,
        `non_unique` Int32,
        `index_schema` String,
        `index_name` Nullable(String),
        `seq_in_index` UInt32,
        `column_name` Nullable(String),
        `collation` Nullable(String),
        `cardinality` Nullable(Int64),
        `sub_part` Nullable(Int64),
        `packed` Nullable(String),
        `nullable` String,
        `index_type` String,
        `comment` String,
        `index_comment` String,
        `is_visible` String,
        `expression` Nullable(String),
        `TABLE_CATALOG` String,
        `TABLE_SCHEMA` String,
        `TABLE_NAME` String,
        `NON_UNIQUE` Int32,
        `INDEX_SCHEMA` String,
        `INDEX_NAME` Nullable(String),
        `SEQ_IN_INDEX` UInt32,
        `COLUMN_NAME` Nullable(String),
        `COLLATION` Nullable(String),
        `CARDINALITY` Nullable(Int64),
        `SUB_PART` Nullable(Int64),
        `PACKED` Nullable(String),
        `NULLABLE` String,
        `INDEX_TYPE` String,
        `COMMENT` String,
        `INDEX_COMMENT` String,
        `IS_VISIBLE` String,
        `EXPRESSION` Nullable(String)
    ) AS
    SELECT
        ''            AS table_catalog,
        ''            AS table_schema,
        ''            AS table_name,
        0             AS non_unique,
        ''            AS index_schema,
        NULL          AS index_name,
        0             AS seq_in_index,
        NULL          AS column_name,
        NULL          AS collation,
        NULL          AS cardinality,
        NULL          AS sub_part,
        NULL          AS packed,
        ''            AS nullable,
        ''            AS index_type,
        ''            AS comment,
        ''            AS index_comment,
        ''            AS is_visible,
        NULL          AS expression,
        table_catalog AS TABLE_CATALOG,
        table_schema  AS TABLE_SCHEMA,
        table_name    AS TABLE_NAME,
        non_unique    AS NON_UNIQUE,
        index_schema  AS INDEX_SCHEMA,
        index_name    AS INDEX_NAME,
        seq_in_index  AS SEQ_IN_INDEX,
        column_name   AS COLUMN_NAME,
        collation     AS COLLATION,
        cardinality   AS CARDINALITY,
        sub_part      AS SUB_PART,
        packed        AS PACKED,
        nullable      AS NULLABLE,
        index_type    AS INDEX_TYPE,
        comment       AS COMMENT,
        index_comment AS INDEX_COMMENT,
        is_visible    AS IS_VISIBLE,
        expression    AS EXPRESSION
    WHERE false; -- make sure this view is always empty
)";

static constexpr std::string_view events = R"(
    ATTACH VIEW events
        (
        `EVENT_CATALOG` String,
        `EVENT_SCHEMA` String,
        `EVENT_NAME` String,
        `DEFINER` String,
        `TIME_ZONE` String,
        `EVENT_BODY` String,
        `EVENT_DEFINITION` Nullable(String),
        `EVENT_TYPE` String,
        `EXECUTE_AT` Nullable(DateTime),
        `INTERVAL_VALUE` Nullable(String),
        `INTERVAL_FIELD` Nullable(String),
        `SQL_MODE` String,
        `STARTS` Nullable(DateTime),
        `ENDS` Nullable(DateTime),
        `STATUS` String,
        `ON_COMPLETION` String,
        `CREATED` Nullable(DateTime),
        `LAST_ALTERED` Nullable(DateTime),
        `LAST_EXECUTED` Nullable(DateTime),
        `EVENT_COMMENT` String,
        `ORIGINATOR` UInt64,
        `CHARACTER_SET_CLIENT` String,
        `COLLATION_CONNECTION` String,
        `DATABASE_COLLATION` String
    ) AS
    SELECT
        '' AS EVENT_CATALOG,
        '' AS EVENT_SCHEMA,
        '' AS EVENT_NAME,
        '' AS DEFINER,
        '' AS TIME_ZONE,
        '' AS EVENT_BODY,
        NULL AS EVENT_DEFINITION,
        '' AS EVENT_TYPE,
        NULL AS EXECUTE_AT,
        NULL AS INTERVAL_VALUE,
        NULL AS INTERVAL_FIELD,
        '' AS SQL_MODE,
        NULL AS STARTS,
        NULL AS ENDS,
        '' AS STATUS,
        '' AS ON_COMPLETION,
        NULL AS CREATED,
        NULL AS LAST_ALTERED,
        NULL AS LAST_EXECUTED,
        '' AS EVENT_COMMENT,
        0 AS ORIGINATOR,
        '' AS CHARACTER_SET_CLIENT,
        '' AS COLLATION_CONNECTION,
        '' AS DATABASE_COLLATION
    WHERE false; -- make sure this view is always empty
)";

static constexpr std::string_view routines = R"(
    ATTACH VIEW routines
        (
        `SPECIFIC_NAME` String,
        `ROUTINE_CATALOG` String,
        `ROUTINE_SCHEMA` String,
        `ROUTINE_NAME` String,
        `ROUTINE_TYPE` String,
        `DATA_TYPE` String,
        `CHARACTER_MAXIMUM_LENGTH` Nullable(Int64),
        `CHARACTER_OCTET_LENGTH` Nullable(Int64),
        `NUMERIC_PRECISION` Nullable(Int64),
        `NUMERIC_SCALE` Nullable(Int64),
        `DATETIME_PRECISION` Nullable(Int64),
        `CHARACTER_SET_NAME` Nullable(String),
        `COLLATION_NAME` Nullable(String),
        `DTD_IDENTIFIER` Nullable(String),
        `ROUTINE_BODY` String,
        `ROUTINE_DEFINITION` Nullable(String),
        `EXTERNAL_NAME` Nullable(String),
        `EXTERNAL_LANGUAGE` Nullable(String),
        `PARAMETER_STYLE` String,
        `IS_DETERMINISTIC` String,
        `SQL_DATA_ACCESS` String,
        `SQL_PATH` Nullable(String),
        `SECURITY_TYPE` String,
        `CREATED` Nullable(DateTime),
        `LAST_ALTERED` Nullable(DateTime),
        `SQL_MODE` String,
        `ROUTINE_COMMENT` String,
        `DEFINER` String,
        `CHARACTER_SET_CLIENT` String,
        `COLLATION_CONNECTION` String,
        `DATABASE_COLLATION` String
    ) AS
    SELECT
        '' AS SPECIFIC_NAME,
        '' AS ROUTINE_CATALOG,
        '' AS ROUTINE_SCHEMA,
        '' AS ROUTINE_NAME,
        '' AS ROUTINE_TYPE,
        '' AS DATA_TYPE,
        NULL AS CHARACTER_MAXIMUM_LENGTH,
        NULL AS CHARACTER_OCTET_LENGTH,
        NULL AS NUMERIC_PRECISION,
        NULL AS NUMERIC_SCALE,
        NULL AS DATETIME_PRECISION,
        NULL AS CHARACTER_SET_NAME,
        NULL AS COLLATION_NAME,
        NULL AS DTD_IDENTIFIER,
        '' AS ROUTINE_BODY,
        NULL AS ROUTINE_DEFINITION,
        NULL AS EXTERNAL_NAME,
        NULL AS EXTERNAL_LANGUAGE,
        '' AS PARAMETER_STYLE,
        '' AS IS_DETERMINISTIC,
        '' AS SQL_DATA_ACCESS,
        NULL AS SQL_PATH,
        '' AS SECURITY_TYPE,
        NULL AS CREATED,
        NULL AS LAST_ALTERED,
        '' AS SQL_MODE,
        '' AS ROUTINE_COMMENT,
        '' AS DEFINER,
        '' AS CHARACTER_SET_CLIENT,
        '' AS COLLATION_CONNECTION,
        '' AS DATABASE_COLLATION
    WHERE false; -- make sure this view is always empty
)";

static constexpr std::string_view triggers = R"(
    ATTACH VIEW triggers
        (
        `TRIGGER_CATALOG` String,
        `TRIGGER_SCHEMA` String,
        `TRIGGER_NAME` String,
        `EVENT_MANIPULATION` String,
        `EVENT_OBJECT_CATALOG` String,
        `EVENT_OBJECT_SCHEMA` String,
        `EVENT_OBJECT_TABLE` String,
        `ACTION_ORDER` Int64,
        `ACTION_CONDITION` Nullable(String),
        `ACTION_STATEMENT` String,
        `ACTION_ORIENTATION` String,
        `ACTION_TIMING` String,
        `ACTION_REFERENCE_OLD_TABLE` Nullable(String),
        `ACTION_REFERENCE_NEW_TABLE` Nullable(String),
        `ACTION_REFERENCE_OLD_ROW` String,
        `ACTION_REFERENCE_NEW_ROW` String,
        `CREATED` Nullable(DateTime),
        `SQL_MODE` String,
        `DEFINER` String,
        `CHARACTER_SET_CLIENT` String,
        `COLLATION_CONNECTION` String,
        `DATABASE_COLLATION` String
    ) AS
    SELECT
        '' AS TRIGGER_CATALOG,
        '' AS TRIGGER_SCHEMA,
        '' AS TRIGGER_NAME,
        '' AS EVENT_MANIPULATION,
        '' AS EVENT_OBJECT_CATALOG,
        '' AS EVENT_OBJECT_SCHEMA,
        '' AS EVENT_OBJECT_TABLE,
        0 AS ACTION_ORDER,
        NULL AS ACTION_CONDITION,
        '' AS ACTION_STATEMENT,
        '' AS ACTION_ORIENTATION,
        '' AS ACTION_TIMING,
        NULL AS ACTION_REFERENCE_OLD_TABLE,
        NULL AS ACTION_REFERENCE_NEW_TABLE,
        '' AS ACTION_REFERENCE_OLD_ROW,
        '' AS ACTION_REFERENCE_NEW_ROW,
        NULL AS CREATED,
        '' AS SQL_MODE,
        '' AS DEFINER,
        '' AS CHARACTER_SET_CLIENT,
        '' AS COLLATION_CONNECTION,
        '' AS DATABASE_COLLATION
    WHERE false; -- make sure this view is always empty
)";

static constexpr std::string_view partitions = R"(
    ATTACH VIEW partitions
        (
        `table_catalog` String,
        `table_schema` String,
        `table_name` String,
        `partition_name` Nullable(String),
        `subpartition_name` Nullable(String),
        `partition_ordinal_position` Nullable(UInt64),
        `subpartition_ordinal_position` Nullable(UInt64),
        `partition_method` Nullable(String),
        `subpartition_method` Nullable(String),
        `partition_expression` Nullable(String),
        `subpartition_expression` Nullable(String),
        `partition_description` Nullable(String),
        `table_rows` Nullable(Int64),
        `avg_row_length` Nullable(Int64),
        `data_length` Nullable(Int64),
        `max_data_length` Nullable(Int64),
        `index_length` Nullable(Int64),
        `data_free` Nullable(Int64),
        `create_time` Nullable(DateTime),
        `update_time` Nullable(DateTime),
        `check_time` Nullable(DateTime),
        `checksum` Nullable(UInt64),
        `partition_comment` Nullable(String),
        `nodegroup` Nullable(Int64),
        `tablespace_name` Nullable(String),
        `TABLE_CATALOG` String,
        `TABLE_SCHEMA` String,
        `TABLE_NAME` String,
        `PARTITION_NAME` Nullable(String),
        `SUBPARTITION_NAME` Nullable(String),
        `PARTITION_ORDINAL_POSITION` Nullable(UInt64),
        `SUBPARTITION_ORDINAL_POSITION` Nullable(UInt64),
        `PARTITION_METHOD` Nullable(String),
        `SUBPARTITION_METHOD` Nullable(String),
        `PARTITION_EXPRESSION` Nullable(String),
        `SUBPARTITION_EXPRESSION` Nullable(String),
        `PARTITION_DESCRIPTION` Nullable(String),
        `TABLE_ROWS` Nullable(Int64),
        `AVG_ROW_LENGTH` Nullable(Int64),
        `DATA_LENGTH` Nullable(Int64),
        `MAX_DATA_LENGTH` Nullable(Int64),
        `INDEX_LENGTH` Nullable(Int64),
        `DATA_FREE` Nullable(Int64),
        `CREATE_TIME` Nullable(DateTime),
        `UPDATE_TIME` Nullable(DateTime),
        `CHECK_TIME` Nullable(DateTime),
        `CHECKSUM` Nullable(UInt64),
        `PARTITION_COMMENT` Nullable(String),
        `NODEGROUP` Nullable(Int64),
        `TABLESPACE_NAME` Nullable(String)
    ) AS
    SELECT 
    -- mysql considers catalog and schema roughly the same.
    -- However CNCH ClickHouse does have a *catalog* server.
    -- For now, stick with mysql interpretation.
        database AS table_catalog,
        database AS table_schema,
        table AS table_name,
        partition AS partition_name,
        NULL AS subpartition_name,
        NULL AS partition_ordinal_position,
        NULL AS subpartition_ordinal_position,
        'HASH' AS partition_method,
        NULL AS subpartition_method,
        trim(splitByRegexp('(PARTITION BY|ORDER BY)', engine_full)[2]) AS partition_expression,
        NULL AS subpartition_expression,
        NULL AS partition_description,
        SUM(rows) AS table_rows,
        SUM(data_compressed_bytes) AS data_length,
        data_length / table_rows AS avg_row_length,
        NULL AS max_data_length,
        SUM(marks_bytes) AS index_length,
        NULL AS data_free,
        NULL AS create_time,
        MAX(modification_time) AS update_time,
        NULL AS check_time,
        NULL AS checksum,
        NULL AS partition_comment,
        NULL AS nodegroup,
        'DEFAULT' AS tablespace_name,
        table_catalog AS TABLE_CATALOG,
        table_schema AS TABLE_SCHEMA,
        table_name AS TABLE_NAME,
        partition_name AS PARTITION_NAME,
        subpartition_name AS SUBPARTITION_NAME,
        partition_ordinal_position AS PARTITION_ORDINAL_POSITION,
        subpartition_ordinal_position AS SUBPARTITION_ORDINAL_POSITION,
        partition_method AS PARTITION_METHOD,
        subpartition_method AS SUBPARTITION_METHOD,
        partition_expression AS PARTITION_EXPRESSION,
        subpartition_expression AS SUBPARTITION_EXPRESSION,
        partition_description AS PARTITION_DESCRIPTION,
        table_rows AS TABLE_ROWS,
        data_length AS DATA_LENGTH,
        avg_row_length AS AVG_ROW_LENGTH,
        max_data_length AS MAX_DATA_LENGTH,
        index_length AS INDEX_LENGTH,
        data_free AS DATA_FREE,
        create_time AS CREATE_TIME,
        update_time AS UPDATE_TIME,
        check_time AS CHECK_TIME,
        checksum AS CHECKSUM,
        partition_comment AS PARTITION_COMMENT,
        nodegroup AS NODEGROUP,
        tablespace_name AS TABLESPACE_NAME
    FROM system.parts, system.tables
    WHERE system.tables.name = system.parts.table
    AND system.parts.active
    AND locate(system.tables.engine_full, 'PARTITION BY') > 0
    GROUP BY
    system.parts.partition,
    system.parts.database,
    system.parts.table,
    system.tables.engine_full
    UNION ALL
    SELECT 
        database AS table_catalog,
        database AS table_schema,
        table AS table_name,
        partition AS partition_name,
        NULL AS subpartition_name,
        NULL AS partition_ordinal_position,
        NULL AS subpartition_ordinal_position,
        'HASH' AS partition_method,
        NULL AS subpartition_method,
        trim(splitByRegexp('(PARTITION BY|ORDER BY)', engine_full)[2]) AS partition_expression,
        NULL AS subpartition_expression,
        NULL AS partition_description,
        SUM(rows_count) AS table_rows,
        -- Since there is no data_compressed_bytes column,
        -- we use bytes_on_disk instead 
        SUM(bytes_on_disk) AS data_length,
        data_length / table_rows AS avg_row_length,
        NULL AS max_data_length,
        -- There is only marks_count column, no marks_bytes
        NULL AS index_length,
        NULL AS data_free,
        NULL AS create_time,
        MAX(commit_time) AS update_time,
        NULL AS check_time,
        NULL AS checksum,
        NULL AS partition_comment,
        NULL AS nodegroup,
        'DEFAULT' AS tablespace_name,
        table_catalog AS TABLE_CATALOG,
        table_schema AS TABLE_SCHEMA,
        table_name AS TABLE_NAME,
        partition_name AS PARTITION_NAME,
        subpartition_name AS SUBPARTITION_NAME,
        partition_ordinal_position AS PARTITION_ORDINAL_POSITION,
        subpartition_ordinal_position AS SUBPARTITION_ORDINAL_POSITION,
        partition_method AS PARTITION_METHOD,
        subpartition_method AS SUBPARTITION_METHOD,
        partition_expression AS PARTITION_EXPRESSION,
        subpartition_expression AS SUBPARTITION_EXPRESSION,
        partition_description AS PARTITION_DESCRIPTION,
        table_rows AS TABLE_ROWS,
        data_length AS DATA_LENGTH,
        avg_row_length AS AVG_ROW_LENGTH,
        max_data_length AS MAX_DATA_LENGTH,
        index_length AS INDEX_LENGTH,
        data_free AS DATA_FREE,
        create_time AS CREATE_TIME,
        update_time AS UPDATE_TIME,
        check_time AS CHECK_TIME,
        checksum AS CHECKSUM,
        partition_comment AS PARTITION_COMMENT,
        nodegroup AS NODEGROUP,
        tablespace_name AS TABLESPACE_NAME
    FROM system.cnch_parts, system.tables
    WHERE system.tables.name = system.cnch_parts.table
    AND system.cnch_parts.part_type = 1 -- visible
    AND locate(system.tables.engine_full, 'PARTITION BY') > 0
    GROUP BY
    system.cnch_parts.partition,
    system.cnch_parts.database,
    system.cnch_parts.table,
    system.tables.engine_full SETTINGS enable_multiple_tables_for_cnch_parts=1;
)";

/// View structures are taken from http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt

static void createInformationSchemaView(ContextMutablePtr context, IDatabase & database, const String & view_name, std::string_view query)
{
    try
    {
        assert(database.getDatabaseName() == DatabaseCatalog::INFORMATION_SCHEMA ||
               database.getDatabaseName() == DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE);
        if (database.getEngineName() != "Memory")
            return;

        String metadata_resource_name = view_name + ".sql";
        if (query.empty())
            return;

        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(),
                                "Attach query from embedded resource " + metadata_resource_name,
                                DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        auto & ast_create = ast->as<ASTCreateQuery &>();
        assert(view_name == ast_create.getTableInfo().getTableName());
        ast_create.attach = false;
        StorageID table_storage_id = ast_create.getTableInfo();
        table_storage_id.database_name = database.getDatabaseName();
        ast_create.setTableInfo(table_storage_id);

        StoragePtr view = createTableFromAST(ast_create, table_storage_id.getDatabaseName(),
                                             database.getTableDataPath(ast_create), context, true).second;
        database.createTable(context, table_storage_id.getTableName(), view, ast);
        ASTPtr ast_upper = ast_create.clone();
        auto & ast_create_upper = ast_upper->as<ASTCreateQuery &>();
        Poco::toUpperInPlace(table_storage_id.table_name);
        ast_create_upper.setTableInfo(table_storage_id);
        StoragePtr view_upper = createTableFromAST(ast_create_upper, table_storage_id.getDatabaseName(),
                                             database.getTableDataPath(ast_create_upper), context, true).second;

        database.createTable(context, table_storage_id.getTableName(), view_upper, ast_upper);

    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void attachInformationSchema(ContextMutablePtr context, IDatabase & information_schema_database)
{
    createInformationSchemaView(context, information_schema_database, "schemata", schemata);
    createInformationSchemaView(context, information_schema_database, "tables", tables);
    createInformationSchemaView(context, information_schema_database, "views", views);
    createInformationSchemaView(context, information_schema_database, "columns", columns);
    createInformationSchemaView(context, information_schema_database, "key_column_usage", key_column_usage);
    createInformationSchemaView(context, information_schema_database, "referential_constraints", referential_constraints);
    createInformationSchemaView(context, information_schema_database, "statistics", statistics);
    createInformationSchemaView(context, information_schema_database, "events", events);
    createInformationSchemaView(context, information_schema_database, "routines", routines);
    createInformationSchemaView(context, information_schema_database, "triggers", triggers);
    createInformationSchemaView(context, information_schema_database, "partitions", partitions);
}

}
