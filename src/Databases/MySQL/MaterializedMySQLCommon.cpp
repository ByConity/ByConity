#include <Databases/MySQL/MaterializedMySQLCommon.h>

#if USE_MYSQL

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/CnchSystemLog.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Storages/IStorage.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_MYSQL_VARIABLE;
}

String toString(const MaterializedMySQLSyncType & type)
{
    switch (type)
    {
        case MaterializedMySQLSyncType::PreparingSync:
            return "PreparingSync";
        case MaterializedMySQLSyncType::FullSync:
            return "FullSync";
        case MaterializedMySQLSyncType::Syncing:
            return "Syncing";
        case MaterializedMySQLSyncType::ManualStopSync:
            return "ManualStopSync";
        case MaterializedMySQLSyncType::ExceptionStopSync:
            return "ExceptionStopSync";
    }
}

MySQLDDLQueryParams parseMySQLDDLQuery(const String & ddl)
{
    MySQLDDLQueryParams params;
    params.query = ddl;
    if (ddl.empty()) return params;

    bool parse_failed = false;
    Tokens tokens(ddl.data(), ddl.data() + ddl.size());
    IParser::Pos pos(tokens, 0);
    Expected expected;
    ASTPtr res;
    ASTPtr table;
    if (ParserKeyword("CREATE TEMPORARY TABLE").ignore(pos, expected) || ParserKeyword("CREATE TABLE").ignore(pos, expected))
    {
        ParserKeyword("IF NOT EXISTS").ignore(pos, expected);
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            parse_failed = true;
        else
            params.query_type = MySQLDDLQueryParams::CREATE_TABLE;
    }
    else if (ParserKeyword("ALTER TABLE").ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            parse_failed = true;
        else
            params.query_type = MySQLDDLQueryParams::ALTER_TABLE;
    }
    else if (ParserKeyword("DROP TABLE").ignore(pos, expected) || ParserKeyword("DROP TEMPORARY TABLE").ignore(pos, expected))
    {
        ParserKeyword("IF EXISTS").ignore(pos, expected);
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            parse_failed = true;
        else
            params.query_type = MySQLDDLQueryParams::DROP_TABLE;
    }
    else if (ParserKeyword("TRUNCATE").ignore(pos, expected))
    {
        ParserKeyword("TABLE").ignore(pos, expected);
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            parse_failed = true;
        else
            params.query_type = MySQLDDLQueryParams::TRUNCATE_TABLE;
    }
    else if (ParserKeyword("RENAME TABLE").ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            parse_failed = true;
        else
        {
            ParserKeyword("TO").ignore(pos, expected);
            ASTPtr to_table;
            if (!ParserCompoundIdentifier(true).parse(pos, to_table, expected))
                parse_failed = true;
            else
            {
                params.query_type = MySQLDDLQueryParams::RENAME_TABLE;

                auto storage_id = to_table->as<ASTTableIdentifier>()->getTableId();
                params.rename_to_database = storage_id.database_name;
                params.rename_to_table = storage_id.table_name;
            }
        }
    }
    else
    {
        parse_failed = true;
    }

    if (!parse_failed)
    {
        auto storage_id = table->as<ASTTableIdentifier>()->getTableId();
        params.execute_database = storage_id.database_name;
        params.execute_table = storage_id.table_name;
    }
    return params;
}

String getNameForMaterializedMySQLManager(const UUID & database_uuid)
{
    WriteBufferFromOwnString out;
    writeText(database_uuid, out);
    writeString("_MANAGER", out);
    return std::move(out.str());
}

String getNamePrefixForMaterializedBinlog(const UUID & database_uuid)
{
    WriteBufferFromOwnString out;
    writeText(database_uuid, out);
    writeString("_BINLOG", out);
    return std::move(out.str());
}

String getNameForMaterializedBinlog(const UUID & database_uuid, const String & table_name)
{
    WriteBufferFromOwnString out;
    writeText(database_uuid, out);
    writeString("_BINLOG", out);
    writeEscapedString("_" + table_name, out);
    return std::move(out.str());
}

namespace MaterializedMySQL
{
/// Some static functions for sync MySQL for the first time
void checkMySQLVariables(const mysqlxx::Pool::Entry & connection, const Settings & settings)
{
    Block variables_header{
        {std::make_shared<DataTypeString>(), "Variable_name"},
        {std::make_shared<DataTypeString>(), "Value"}
    };

    const String & check_query = "SHOW VARIABLES;";

    StreamSettings mysql_input_stream_settings(settings, false, true);
    MySQLBlockInputStream variables_input(connection, check_query, variables_header, mysql_input_stream_settings);

    std::unordered_map<String, String> variables_error_message{
        {"log_bin", "ON"},
        {"binlog_format", "ROW"},
        {"binlog_row_image", "FULL"},
        {"default_authentication_plugin", "mysql_native_password"},
        {"log_bin_use_v1_row_events", "OFF"}
    };

    while (Block variables_block = variables_input.read())
    {
        ColumnPtr variable_name_column = variables_block.getByName("Variable_name").column;
        ColumnPtr variable_value_column = variables_block.getByName("Value").column;

        for (size_t index = 0; index < variables_block.rows(); ++index)
        {
            const auto & error_message_it = variables_error_message.find(variable_name_column->getDataAt(index).toString());
            const String variable_val = variable_value_column->getDataAt(index).toString();

            if (error_message_it != variables_error_message.end() && variable_val == error_message_it->second)
                variables_error_message.erase(error_message_it);
        }
    }

    if  (!variables_error_message.empty())
    {
        bool first = true;
        String error_message_str = "Illegal MySQL variables, the MaterializeMySQL engine requires ";
        for (const auto & [variable_name, variable_error_val] : variables_error_message)
        {
            error_message_str += ((first ? "" : ", ") + variable_name + "='" + variable_error_val + "'");

            if (first)
                first = false;
        }
        throw Exception(error_message_str, ErrorCodes::ILLEGAL_MYSQL_VARIABLE);
    }
}

ContextMutablePtr createQueryContext(ContextPtr context, bool init_query)
{
    Settings new_query_settings = context->getSettings();
    new_query_settings.insert_allow_materialized_columns = true;

    /// To avoid call AST::format
    /// TODO: We need to implement the format function for MySQLAST
    new_query_settings.enable_global_with_statement = false;
    new_query_settings.force_manipulate_materialized_mysql_table = true;

    auto query_context = Context::createCopy(context);
    query_context->setSettings(new_query_settings);
    if (init_query)
        CurrentThread::QueryScope query_scope(query_context);

    query_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    query_context->setCurrentQueryId(""); // generate random query_id
    return query_context;
}

MaterializedMySQLLogElement createMaterializeMySQLLog(MaterializedMySQLLogElement::EventSource event_source, MaterializedMySQLLogElement::Type type, String database, NameSet tables)
{
    MaterializedMySQLLogElement elem;
    if (event_source == MaterializedMySQLLogElement::EventSource::SYNC_MANAGER)
        elem.cnch_database = database;
    else
    {
        elem.database = database;
        elem.tables = tables;
    }
    elem.type = type;
    elem.event_time = time(nullptr);
    elem.event_source = event_source;
    return elem;
}

void recordException(MaterializedMySQLLogElement::EventSource event_source, ContextPtr context, String database, NameSet tables, bool create_log, const String & resync_table)
{
    String error_msg = getCurrentExceptionMessage(false);
    if (!create_log)
        return;
    if (auto materialize_mysql_log = context->getCloudMaterializedMySQLLog())
    {
        auto current_log = MaterializedMySQL::createMaterializeMySQLLog(event_source, MaterializedMySQLLogElement::ERROR, database, tables);
        current_log.has_error = 1;
        current_log.event_msg = error_msg;
        current_log.resync_table = resync_table;
        materialize_mysql_log->add(current_log);
    }
}

}

}

#endif
