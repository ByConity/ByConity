#include <Databases/DatabaseOnDisk.h>
#include <Storages/System/attachMySQLTables.h>
#include <Storages/System/attachSystemTablesImpl.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

static constexpr std::string_view user = R"(
    ATTACH VIEW user
    (
     `host` String NOT NULL,
     `user` String NOT NULL,
     `select_priv` String NOT NULL,
     `insert_priv` String NOT NULL,
     `update_priv` String NOT NULL,
     `delete_priv` String NOT NULL,
     `create_priv` String NOT NULL,
     `drop_priv` String NOT NULL,
     `reload_priv` String NOT NULL,
     `shutdown_priv` String NOT NULL,
     `process_priv` String NOT NULL,
     `file_priv` String NOT NULL,
     `grant_priv` String NOT NULL,
     `references_priv` String NOT NULL,
     `index_priv` String NOT NULL,
     `alter_priv` String NOT NULL,
     `show_db_priv` String NOT NULL,
     `super_priv` String NOT NULL,
     `create_tmp_table_priv` String NOT NULL,
     `lock_tables_priv` String NOT NULL,
     `execute_priv` String NOT NULL,
     `repl_slave_priv` String NOT NULL,
     `repl_client_priv` String NOT NULL,
     `create_view_priv` String NOT NULL,
     `show_view_priv` String NOT NULL,
     `create_routine_priv` String NOT NULL,
     `alter_routine_priv` String NOT NULL,
     `create_user_priv` String NOT NULL,
     `event_priv` String NOT NULL,
     `trigger_priv` String NOT NULL,
     `create_tablespace_priv` String NOT NULL,
     `ssl_type` String NOT NULL,
     `ssl_cipher` String NOT NULL,
     `x509_issuer` String NOT NULL,
     `x509_subject` String NOT NULL,
     `max_questions` int NOT NULL,
     `max_updates` int NOT NULL,
     `max_connections` int NOT NULL,
     `max_user_connections` int NOT NULL,
     `plugin` String NOT NULL,
     `authentication_string` String NOT NULL,
     `password_expired` String NOT NULL,
     `HOST` String NOT NULL,
     `USER` String NOT NULL,
     `SELECT_PRIV` String NOT NULL,
     `INSERT_PRIV` String NOT NULL,
     `UPDATE_PRIV` String NOT NULL,
     `DELETE_PRIV` String NOT NULL,
     `CREATE_PRIV` String NOT NULL,
     `DROP_PRIV` String NOT NULL,
     `RELOAD_PRIV` String NOT NULL,
     `SHUTDOWN_PRIV` String NOT NULL,
     `PROCESS_PRIV` String NOT NULL,
     `FILE_PRIV` String NOT NULL,
     `GRANT_PRIV` String NOT NULL,
     `REFERENCES_PRIV` String NOT NULL,
     `INDEX_PRIV` String NOT NULL,
     `ALTER_PRIV` String NOT NULL,
     `SHOW_DB_PRIV` String NOT NULL,
     `SUPER_PRIV` String NOT NULL,
     `CREATE_TMP_TABLE_PRIV` String NOT NULL,
     `LOCK_TABLES_PRIV` String NOT NULL,
     `EXECUTE_PRIV` String NOT NULL,
     `REPL_SLAVE_PRIV` String NOT NULL,
     `REPL_CLIENT_PRIV` String NOT NULL,
     `CREATE_VIEW_PRIV` String NOT NULL,
     `SHOW_VIEW_PRIV` String NOT NULL,
     `CREATE_ROUTINE_PRIV` String NOT NULL,
     `ALTER_ROUTINE_PRIV` String NOT NULL,
     `CREATE_USER_PRIV` String NOT NULL,
     `EVENT_PRIV` String NOT NULL,
     `TRIGGER_PRIV` String NOT NULL,
     `CREATE_TABLESPACE_PRIV` String NOT NULL,
     `SSL_TYPE` String NOT NULL,
     `SSL_CIPHER` String NOT NULL,
     `X509_ISSUER` String NOT NULL,
     `X509_SUBJECT` String NOT NULL,
     `MAX_QUESTIONS` int NOT NULL,
     `MAX_UPDATES` int NOT NULL,
     `MAX_CONNECTIONS` int NOT NULL,
     `MAX_USER_CONNECTIONS` int NOT NULL,
     `PLUGIN` String NOT NULL,
     `AUTHENTICATION_STRING` String NOT NULL,
     `PASSWORD_EXPIRED` String NOT NULL,
     `Host` String NOT NULL,
     `User` String NOT NULL,
     `Select_priv` String NOT NULL,
     `Insert_priv` String NOT NULL,
     `Update_priv` String NOT NULL,
     `Delete_priv` String NOT NULL,
     `Create_priv` String NOT NULL,
     `Drop_priv` String NOT NULL,
     `Reload_priv` String NOT NULL,
     `Shutdown_priv` String NOT NULL,
     `Process_priv` String NOT NULL,
     `File_priv` String NOT NULL,
     `Grant_priv` String NOT NULL,
     `References_priv` String NOT NULL,
     `Index_priv` String NOT NULL,
     `Alter_priv` String NOT NULL,
     `Show_db_priv` String NOT NULL,
     `Super_priv` String NOT NULL,
     `Create_tmp_table_priv` String NOT NULL,
     `Lock_tables_priv` String NOT NULL,
     `Execute_priv` String NOT NULL,
     `Repl_slave_priv` String NOT NULL,
     `Repl_client_priv` String NOT NULL,
     `Create_view_priv` String NOT NULL,
     `Show_view_priv` String NOT NULL,
     `Create_routine_priv` String NOT NULL,
     `Alter_routine_priv` String NOT NULL,
     `Create_user_priv` String NOT NULL,
     `Event_priv` String NOT NULL,
     `Trigger_priv` String NOT NULL,
     `Create_tablespace_priv` String NOT NULL
    ) AS
    SELECT
     *,
     host as HOST,
     user as USER,
     select_priv     as SELECT_PRIV,
     insert_priv     as INSERT_PRIV,
     update_priv     as UPDATE_PRIV,
     delete_priv     as DELETE_PRIV,
     create_priv     as CREATE_PRIV,
     drop_priv       as DROP_PRIV,
     reload_priv     as RELOAD_PRIV,
     shutdown_priv   as SHUTDOWN_PRIV,
     process_priv    as PROCESS_PRIV,
     file_priv       as FILE_PRIV,
     grant_priv      as GRANT_PRIV,
     references_priv as REFERENCES_PRIV,
     index_priv      as INDEX_PRIV,
     alter_priv      as ALTER_PRIV,
     show_db_priv    as SHOW_DB_PRIV,
     super_priv      as SUPER_PRIV,
     create_tmp_table_priv as CREATE_TMP_TABLE_PRIV,
     lock_tables_priv as LOCK_TABLES_PRIV,
     execute_priv    as EXECUTE_PRIV,
     repl_slave_priv as REPL_SLAVE_PRIV,
     repl_client_priv as REPL_CLIENT_PRIV,
     create_view_priv as CREATE_VIEW_PRIV,
     show_view_priv  as SHOW_VIEW_PRIV,
     create_routine_priv as CREATE_ROUTINE_PRIV,
     alter_routine_priv as ALTER_ROUTINE_PRIV,
     create_user_priv as CREATE_USER_PRIV,
     event_priv      as EVENT_PRIV,
     trigger_priv    as TRIGGER_PRIV,
     create_tablespace_priv as CREATE_TABLESPACE_PRIV,
     ssl_type        as SSL_TYPE,
     ssl_cipher      as SSL_CIPHER,
     x509_issuer     as X509_ISSUER,
     x509_subject    as X509_SUBJECT,
     max_questions   as MAX_QUESTIONS,
     max_updates     as MAX_UPDATES,
     max_connections as MAX_CONNECTIONS,
     max_user_connections as MAX_USER_CONNECTIONS,
     plugin          as PLUGIN,
     authentication_string as AUTHENTICATION_STRING,
     password_expired as PASSWORD_EXPIRED,
     host as Host,
     user as User,
     select_priv     as Select_priv,
     insert_priv     as Insert_priv,
     update_priv     as Update_priv,
     delete_priv     as Delete_priv,
     create_priv     as Create_priv,
     drop_priv       as Drop_priv,
     reload_priv     as Reload_priv,
     shutdown_priv   as Shutdown_priv,
     process_priv    as Process_priv,
     file_priv       as File_priv,
     grant_priv      as Grant_priv,
     references_priv as References_priv,
     index_priv      as Index_priv,
     alter_priv      as Alter_priv,
     show_db_priv    as Show_db_priv,
     super_priv      as Super_priv,
     create_tmp_table_priv as Create_tmp_table_priv,
     lock_tables_priv as Lock_tables_priv,
     execute_priv    as Execute_priv,
     repl_slave_priv as Repl_slave_priv,
     repl_client_priv as Repl_client_priv,
     create_view_priv as Create_view_priv,
     show_view_priv  as Show_view_priv,
     create_routine_priv as Create_routine_priv,
     alter_routine_priv as Alter_routine_priv,
     create_user_priv as Create_user_priv,
     event_priv      as Event_priv,
     trigger_priv    as Trigger_priv,
     create_tablespace_priv as Create_tablespace_priv
    FROM system.cnch_user_priv
)";


static constexpr std::string_view db = R"(
    ATTACH VIEW db
    (
        `host` String NOT NULL,
        `db` String NOT NULL,
        `user` String NOT NULL,
        `select_priv` String NOT NULL,
        `insert_priv` String NOT NULL,
        `update_priv` String NOT NULL,
        `delete_priv` String NOT NULL,
        `create_priv` String NOT NULL,
        `drop_priv` String NOT NULL,
        `grant_priv` String NOT NULL,
        `references_priv` String NOT NULL,
        `index_priv` String NOT NULL,
        `alter_priv` String NOT NULL,
        `create_tmp_table_priv` String NOT NULL,
        `lock_tables_priv` String NOT NULL,
        `create_view_priv` String NOT NULL,
        `show_view_priv` String NOT NULL,
        `create_routine_priv` String NOT NULL,
        `alter_routine_priv` String NOT NULL,
        `execute_priv` String NOT NULL,
        `event_priv` String NOT NULL,
        `trigger_priv` String NOT NULL,
        `HOST` String NOT NULL,
        `DB` String NOT NULL,
        `USER` String NOT NULL,
        `SELECT_PRIV` String NOT NULL,
        `INSERT_PRIV` String NOT NULL,
        `UPDATE_PRIV` String NOT NULL,
        `DELETE_PRIV` String NOT NULL,
        `CREATE_PRIV` String NOT NULL,
        `DROP_PRIV` String NOT NULL,
        `GRANT_PRIV` String NOT NULL,
        `REFERENCES_PRIV` String NOT NULL,
        `INDEX_PRIV` String NOT NULL,
        `ALTER_PRIV` String NOT NULL,
        `CREATE_TMP_TABLE_PRIV` String NOT NULL,
        `LOCK_TABLES_PRIV` String NOT NULL,
        `CREATE_VIEW_PRIV` String NOT NULL,
        `SHOW_VIEW_PRIV` String NOT NULL,
        `CREATE_ROUTINE_PRIV` String NOT NULL,
        `ALTER_ROUTINE_PRIV` String NOT NULL,
        `EXECUTE_PRIV` String NOT NULL,
        `EVENT_PRIV` String NOT NULL,
        `TRIGGER_PRIV` String NOT NULL,
        `Host` String NOT NULL,
        `Db` String NOT NULL,
        `User` String NOT NULL,
        `Select_priv` String NOT NULL,
        `Insert_priv` String NOT NULL,
        `Update_priv` String NOT NULL,
        `Delete_priv` String NOT NULL,
        `Create_priv` String NOT NULL,
        `Drop_priv` String NOT NULL,
        `Grant_priv` String NOT NULL,
        `References_priv` String NOT NULL,
        `Index_priv` String NOT NULL,
        `Alter_priv` String NOT NULL,
        `Create_tmp_table_priv` String NOT NULL,
        `Lock_tables_priv` String NOT NULL,
        `Create_view_priv` String NOT NULL,
        `Show_view_priv` String NOT NULL,
        `Create_routine_priv` String NOT NULL,
        `Alter_routine_priv` String NOT NULL,
        `Execute_priv` String NOT NULL,
        `Event_priv` String NOT NULL,
        `Trigger_priv` String NOT NULL
    ) AS
    SELECT
        *,
        host AS HOST,
        db AS DB,
        user AS USER,
        select_priv AS SELECT_PRIV,
        insert_priv AS INSERT_PRIV,
        update_priv AS UPDATE_PRIV,
        delete_priv AS DELETE_PRIV,
        create_priv AS CREATE_PRIV,
        drop_priv AS DROP_PRIV,
        grant_priv AS GRANT_PRIV,
        references_priv AS REFERENCES_PRIV,
        index_priv AS INDEX_PRIV,
        alter_priv AS ALTER_PRIV,
        create_tmp_table_priv AS CREATE_TMP_TABLE_PRIV,
        lock_tables_priv AS LOCK_TABLES_PRIV,
        create_view_priv AS CREATE_VIEW_PRIV,
        show_view_priv AS SHOW_VIEW_PRIV,
        create_routine_priv AS CREATE_ROUTINE_PRIV,
        alter_routine_priv AS ALTER_ROUTINE_PRIV,
        execute_priv AS EXECUTE_PRIV,
        event_priv AS EVENT_PRIV,
        trigger_priv AS TRIGGER_PRIV,
        host AS Host,
        db AS Db,
        user AS User,
        select_priv AS Select_priv,
        insert_priv AS Insert_priv,
        update_priv AS Update_priv,
        delete_priv AS Delete_priv,
        create_priv AS Create_priv,
        drop_priv AS Drop_priv,
        grant_priv AS Grant_priv,
        references_priv AS References_priv,
        index_priv AS Index_priv,
        alter_priv AS Alter_priv,
        create_tmp_table_priv AS Create_tmp_table_priv,
        lock_tables_priv AS Lock_tables_priv,
        create_view_priv AS Create_view_priv,
        show_view_priv AS Show_view_priv,
        create_routine_priv AS Create_routine_priv,
        alter_routine_priv AS Alter_routine_priv,
        execute_priv AS Execute_priv,
        event_priv AS Event_priv,
        trigger_priv AS Trigger_priv
    FROM system.cnch_db_priv
)";


static constexpr std::string_view columns_priv = R"(
    ATTACH VIEW columns_priv (
        host         Nullable(String),
        db           Nullable(String),
        user         Nullable(String),
        table_name   Nullable(String),
        column_name  Nullable(String),
        timestamp    DateTime64(3),
        column_priv  Nullable(String),
        HOST         Nullable(String),
        DB           Nullable(String),
        USER         Nullable(String),
        TABLE_NAME   Nullable(String),
        COLUMN_NAME  Nullable(String),
        TIMESTAMP    DateTime64(3),
        COLUMN_PRIV  Nullable(String),
        Host         Nullable(String),
        Db           Nullable(String),
        User         Nullable(String),
        Table_name   Nullable(String),
        Column_name  Nullable(String),
        Timestamp    DateTime64(3),
        Column_priv  Nullable(String)
    ) AS
        SELECT
           NULL as host,
           NULL as db,
           NULL as user,
           NULL as table_name,
           NULL as column_name,
           NULL as timestamp,
           NULL as column_priv,
           NULL as HOST,
           NULL as DB,
           NULL as USER,
           NULL as TABLE_NAME,
           NULL as COLUMN_NAME,
           NULL as TIMESTAMP,
           NULL as COLUMN_PRIV,
           NULL as Host,
           NULL as Db,
           NULL as User,
           NULL as Table_name,
           NULL as Column_name,
           NULL as Timestamp,
           NULL as Column_priv
         WHERE false
)";

static constexpr std::string_view tables_priv = R"(
    ATTACH VIEW tables_priv (
        host         Nullable(String),
        db           Nullable(String),
        user         Nullable(String),
        table_name   Nullable(String),
        grantor      Nullable(String),
        timestamp    DateTime64(3),
        table_priv   Nullable(String),
        column_priv  Nullable(String),
        HOST         Nullable(String),
        DB           Nullable(String),
        USER         Nullable(String),
        TABLE_NAME   Nullable(String),
        GRANTOR      Nullable(String),
        TIMESTAMP    DateTime64(3),
        TABLE_PRIV   Nullable(String),
        COLUMN_PRIV  Nullable(String),
        Host         Nullable(String),
        Db           Nullable(String),
        User         Nullable(String),
        Table_name   Nullable(String),
        Grantor      Nullable(String),
        Timestamp    DateTime64(3),
        Table_priv   Nullable(String),
        Column_priv  Nullable(String)
    ) AS
    SELECT
        NULL AS host         ,
        NULL AS db           ,
        NULL AS user         ,
        NULL AS table_name   ,
        NULL AS grantor      ,
        NULL AS timestamp    ,
        NULL AS table_priv   ,
        NULL AS column_priv  ,
        NULL AS HOST         ,
        NULL AS DB           ,
        NULL AS USER         ,
        NULL AS TABLE_NAME   ,
        NULL AS GRANTOR      ,
        NULL AS TIMESTAMP    ,
        NULL AS TABLE_PRIV   ,
        NULL AS COLUMN_PRIV  ,
        NULL AS Host         ,
        NULL AS Db           ,
        NULL AS User         ,
        NULL AS Table_name   ,
        NULL AS Grantor      ,
        NULL AS Timestamp    ,
        NULL AS Table_priv   ,
        NULL AS Column_priv
    WHERE false
)";

static void createMySQLView(ContextMutablePtr context, IDatabase & database, const String & view_name, std::string_view query)
{
    try
    {
        assert(database.getDatabaseName() == DatabaseCatalog::MYSQL ||
               database.getDatabaseName() == DatabaseCatalog::MYSQL_UPPERCASE);
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

void attachMySQL(ContextMutablePtr context, IDatabase & mysql_database)
{
    createMySQLView(context, mysql_database, "user", user);
    createMySQLView(context, mysql_database, "db", db);
    createMySQLView(context, mysql_database, "columns_priv", columns_priv);
    createMySQLView(context, mysql_database, "tables_priv", tables_priv);
}

}
