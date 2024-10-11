#include "MySQLHandler.h"

#include <limits>
#include <common/scope_guard.h>
#include <Columns/ColumnVector.h>
#include <Common/NetException.h>
#include <Common/OpenSSLHelpers.h>
#include <Core/MySQL/Authentication.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsConnection.h>
#include <Core/MySQL/PacketsPreparedStatements.h>
#include <Core/MySQL/PacketsProtocolText.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/copyData.h>
#include <Interpreters/executeQuery.h>
#include <IO/copyData.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Server/TCPServer.h>
#include <Storages/IStorage.h>
#include <boost/algorithm/string/replace.hpp>
#include <regex>
#include <Access/User.h>
#include <Access/AccessControlManager.h>
#include <Common/setThreadName.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif
#include <re2/re2.h>

#if USE_SSL
#    include <Poco/Crypto/CipherFactory.h>
#    include <Poco/Crypto/RSAKey.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>

#endif

namespace DB
{

using namespace MySQLProtocol;
using namespace MySQLProtocol::Generic;
using namespace MySQLProtocol::ProtocolText;
using namespace MySQLProtocol::ConnectionPhase;
using namespace MySQLProtocol::PreparedStatements;

#if USE_SSL
using Poco::Net::SecureStreamSocket;
using Poco::Net::SSLManager;
#endif

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNSUPPORTED_METHOD;
}


static const size_t PACKET_HEADER_SIZE = 4;
static const size_t SSL_REQUEST_PAYLOAD_SIZE = 32;

static String selectEmptyReplacementQuery(const String & query, const String & prefix);
static String selectEmptySetQuery(const String & /*query*/, const String & /*prefix*/);
static String showIndexReplacementQuery(const String & query, const String & prefix);
static String showTableStatusReplacementQuery(const String & query, const String & prefix);
static String killConnectionIdReplacementQuery(const String & query, const String & prefix);
static std::optional<String> setSettingReplacementQuery(const String & query, const String & mysql_setting, const String & native_setting);
// static String showCreateTableReplacementQuery(const String & query, const String & prefix);

// For MySQL workbench
static String showVariableReplacementQuery(const String & query, const String & prefix)
{
    std::regex pattern(prefix + R"( LIKE (.*))");
    std::smatch match;

    /// for 'show variables;'
    if (!std::regex_search(query, match, pattern))
    {
        return "SELECT name AS Variable_name, value AS Value FROM system.settings";
        // return query;
    }

    if (match[1].matched)
    {
        String name = match[1].str();
        return "SELECT " + name + " AS Variable_name, globalVariable(" + name + ") AS Value";
    }
    else
    {
        return "SELECT * FROM null('Variable_name String, Value String')";
    }
}

static String showVariableWhereReplacementQuery(const String & query, const String & prefix)
{
    if (query.size() <= prefix.size())
        return query;

    RE2 pattern("'(.*?)'");
    std::vector<std::string> variable_names;
    re2::StringPiece input_sp(query.data() + prefix.length()); // RE2 uses its own StringPiece type rather than std::string
    std::string match;
    while (RE2::FindAndConsume(&input_sp, pattern, &match))
        variable_names.push_back(match);

    std::string q;
    for (const auto& name : variable_names) {
        if (!q.empty())
            q += " UNION ALL ";
        q += "SELECT '" + name + "' AS Variable_name, toString(globalVariable('" + name + "')) AS Value";
    }

    return q;
}

template<const char * q>
struct ReplaceWith
{
    // Simple replacement
    static String fn(const String & query, const String & prefix)
    {
        if (query.size() <= prefix.size())
            return q;

        String suffix = query.data() + prefix.length();
        return String(q) + suffix;
    }

    // Replace identifier with string
    static String id2str(const String & query, const String & prefix)
    {
        if (query.size() <= prefix.size())
            return q;

        // TODO: replace identifier without `
        // TODO: support FROM database
        String suffix = query.data() + prefix.length();

        auto begin = suffix.find('`');
        if (begin != String::npos)
        {
            auto end = suffix.find('`', begin + 1);
            if (end != String::npos)
            {
                suffix[begin] = '\'';
                suffix[end] = '\'';
            }
        }

        return String(q) + suffix;
    }
};

MySQLHandler::MySQLHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_,
    bool ssl_enabled, uint32_t connection_id_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , log(getRawLogger("MySQLHandler"))
    , connection_id(connection_id_)
    , connection_context(Context::createCopy(server.context()))
    , auth_plugin(new MySQLProtocol::Authentication::Native41())
{
    server_capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_CONNECT_WITH_DB | CLIENT_DEPRECATE_EOF;
    if (ssl_enabled)
        server_capabilities |= CLIENT_SSL;

    static constexpr const char SHOW_CHARSET[] = "SELECT 'utf8mb4' AS charset, 'UTF-8 Unicode' AS Description, 'utf8mb4_0900_ai_ci' AS `Default collation`, 4 AS Maxlen";
    static constexpr const char SHOW_COLLATION[] = "SELECT 'utf8_general_ci' AS collation, 'utf8' AS charset, 33 AS id, 'Yes' AS default, 'Yes' AS Compiled, 1 AS Sortlen, 'NO PAD' AS Pad_attribute "
                                                   "UNION SELECT 'binary' AS collation, 'binary' AS charset, 63 AS id, 'Yes' AS default, 'Yes' AS Compiled, 1 AS Sortlen, 'NO PAD' AS Pad_attribute "
                                                   "UNION SELECT 'utf8mb4_0900_ai_ci' AS collation, 'utf8mb4' AS Charset, '255' AS Id, 'Yes' AS Default, 'Yes' AS Compiled, 0 AS Sortlen, 'NO PAD' AS Pad_attribute";
    static constexpr const char SHOW_ENGINES[] = "SELECT name AS Engine, 'Yes' AS Support, concat(name, ' engine') AS Comment, 'NO' AS Transcations,  'NO' AS XA, 'NO' AS Savepoints FROM system.table_engines";

    static constexpr const char SHOW_PRIVILEGES[] = "SELECT '' AS Privilege, '' AS Context, '' AS Comment";
    static constexpr const char ANALYZE_TABLE[] = "CREATE STATS ";
    static constexpr const char SHOW_CREATE_SCHEMA[] = "SHOW CREATE DATABASE ";

    queries_replacements.emplace_back("ALTER DATABASE", selectEmptyReplacementQuery);
    queries_replacements.emplace_back("ANALYZE TABLE", ReplaceWith<ANALYZE_TABLE>::fn);
    queries_replacements.emplace_back("COMMIT", selectEmptyReplacementQuery);
    queries_replacements.emplace_back("KILL QUERY ", killConnectionIdReplacementQuery);
    queries_replacements.emplace_back("LOCK TABLE", selectEmptySetQuery);
    queries_replacements.emplace_back("UNLOCK TABLE", selectEmptySetQuery);
    queries_replacements.emplace_back("REPAIR TABLE", selectEmptyReplacementQuery);
    queries_replacements.emplace_back("ROLLBACK", selectEmptyReplacementQuery);
    queries_replacements.emplace_back("SHOW CHARACTER SET", ReplaceWith<SHOW_CHARSET>::fn);
    queries_replacements.emplace_back("SHOW CHARSET", ReplaceWith<SHOW_CHARSET>::fn);
    queries_replacements.emplace_back("SHOW COLLATION", ReplaceWith<SHOW_COLLATION>::fn);
    queries_replacements.emplace_back("SHOW CREATE SCHEMA", ReplaceWith<SHOW_CREATE_SCHEMA>::fn);
    queries_replacements.emplace_back("SHOW ENGINES", ReplaceWith<SHOW_ENGINES>::fn);
    queries_replacements.emplace_back("SHOW EVENTS", selectEmptySetQuery);
    queries_replacements.emplace_back("SHOW FUNCTION STATUS", selectEmptySetQuery);
    queries_replacements.emplace_back("SHOW GLOBAL STATUS", showVariableReplacementQuery);
    queries_replacements.emplace_back("SHOW GLOBAL VARIABLES", showVariableReplacementQuery);
    queries_replacements.emplace_back("SHOW INDEXES", showIndexReplacementQuery);
    queries_replacements.emplace_back("SHOW INDEX", showIndexReplacementQuery);
    queries_replacements.emplace_back("SHOW KEYS", showIndexReplacementQuery);
    queries_replacements.emplace_back("SHOW PLUGINS", selectEmptyReplacementQuery);
    queries_replacements.emplace_back("SHOW PRIVILEGES", ReplaceWith<SHOW_PRIVILEGES>::fn);
    queries_replacements.emplace_back("SHOW PROCEDURE STATUS", selectEmptySetQuery);
    queries_replacements.emplace_back("SHOW SESSION STATUS", showVariableReplacementQuery);
    queries_replacements.emplace_back("SHOW SESSION VARIABLES", showVariableReplacementQuery);
    queries_replacements.emplace_back("SHOW STATUS", showVariableReplacementQuery);
    queries_replacements.emplace_back("SHOW TABLE STATUS", showTableStatusReplacementQuery);
    queries_replacements.emplace_back("SHOW TRIGGERS", selectEmptySetQuery);
    queries_replacements.emplace_back("SHOW VARIABLES WHERE ", showVariableWhereReplacementQuery);
    queries_replacements.emplace_back("SHOW VARIABLES", selectEmptyReplacementQuery);
    queries_replacements.emplace_back("SHOW VARIABLES", showVariableReplacementQuery);

    settings_replacements.emplace_back("SQL_SELECT_LIMIT=DEFAULT", "limit = 0");
    settings_replacements.emplace_back("SQL_SELECT_LIMIT", "limit");
    settings_replacements.emplace_back("NET_WRITE_TIMEOUT", "send_timeout");
    settings_replacements.emplace_back("NET_READ_TIMEOUT", "receive_timeout");
    settings_replacements.emplace_back("CHARACTER SET utf8", "SQL_CHARSET='utf8mb4'");
    settings_replacements.emplace_back("SESSION TRANSACTION READ ONLY", "SQL_TXN_READ_ONLY=1");
    settings_replacements.emplace_back("AUTOCOMMIT", "SQL_AUTOCOMMIT");
    settings_replacements.emplace_back("PROFILING", "SQL_PROFILING");
}

void MySQLHandler::run()
{
    setThreadName("MySQLHandler");
    ThreadStatus thread_status;
    connection_context->makeSessionContext();
    connection_context->getClientInfo().interface = ClientInfo::Interface::MYSQL;
    connection_context->setDefaultFormat("MySQLWire");
    connection_context->getClientInfo().connection_id = connection_id;

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
    packet_endpoint = std::make_shared<MySQLProtocol::PacketEndpoint>(*in, *out, sequence_id);

    try
    {
        Handshake handshake(server_capabilities, connection_id, "5.1.0",
            auth_plugin->getName(), auth_plugin->getAuthPluginData(), CharacterSet::utf8_general_ci);
        packet_endpoint->sendPacket<Handshake>(handshake, true);

        LOG_TRACE(log, "Sent handshake");

        HandshakeResponse handshake_response;
        finishHandshake(handshake_response);
        client_capabilities = handshake_response.capability_flags;
        max_packet_size = handshake_response.max_packet_size ? handshake_response.max_packet_size : MAX_PACKET_LENGTH;

        LOG_TRACE(log,
            "Capabilities: {}, max_packet_size: {}, character_set: {}, user: {}, auth_response length: {}, database: {}, auth_plugin_name: {}",
            handshake_response.capability_flags,
            handshake_response.max_packet_size,
            static_cast<int>(handshake_response.character_set),
            handshake_response.username,
            handshake_response.auth_response.length(),
            handshake_response.database,
            handshake_response.auth_plugin_name);

        if (!(client_capabilities & CLIENT_PROTOCOL_41))
            throw Exception("Required capability: CLIENT_PROTOCOL_41.", ErrorCodes::MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES);

        try
        {
            auto &default_database = handshake_response.database;
            if (!default_database.empty())
            {
                //CNCH multi-tenant default database pattern from gateway client: {tenant_id}`{default_database}
                if (auto pos = default_database.find('`'); pos != String::npos)
                {
                    connection_context->setSetting("tenant_id", String(default_database.c_str(), pos)); /// {tenant_id}`*
                    connection_context->setTenantId(String(default_database.c_str(), pos));
                    if (pos + 1 != default_database.size()) ///multi-tenant default database storage pattern: {tenant_id}.{default_database}
                        default_database[pos] = '.';
                    else /// {tenant_id}`
                        default_database.clear();
                }
                else if (!default_database.empty() && !connection_context->getTenantId().empty())
                {
                    default_database = connection_context->getTenantId() + '.' + default_database;
                }

                if (!default_database.empty())
                    connection_context->setCurrentDatabase(default_database);
            }
            SettingsChanges setting_changes;
            setting_changes.emplace_back("dialect_type", String("MYSQL"));
            connection_context->applySettingsChanges(setting_changes);
            connection_context->setCurrentQueryId(fmt::format("mysql:{}", connection_id));
            auto & client_info = connection_context->getClientInfo();
            client_info.initial_query_id = client_info.current_query_id;
        }
        catch (const Exception & exc)
        {
            log->log(exc);
            packet_endpoint->sendPacket(ERRPacket(exc.code(), "HY000", exc.message()), true);
        }

        handshake_response.username = connection_context->formatUserName(handshake_response.username);
        authenticate(handshake_response.username, handshake_response.auth_plugin_name, handshake_response.auth_response);

        connection_context->getClientInfo().initial_user = handshake_response.username;

        OKPacket ok_packet(0, handshake_response.capability_flags, 0, 0, 0);
        packet_endpoint->sendPacket(ok_packet, true);

        while (tcp_server.isOpen())
        {
            packet_endpoint->resetSequenceId();
            MySQLPacketPayloadReadBuffer payload = packet_endpoint->getPayload();

            while (!in->poll(1000000))
                if (!tcp_server.isOpen())
                    return;

            char command = 0;
            payload.readStrict(command);

            // For commands which are executed without MemoryTracker.
            LimitReadBuffer limited_payload(payload, 10000, true, "too long MySQL packet.");

            LOG_DEBUG(log, "Received command: {}. Connection id: {}.",
                static_cast<int>(static_cast<unsigned char>(command)), connection_id);

            if (!tcp_server.isOpen())
                return;

            try
            {
                switch (command)
                {
                    case COM_QUIT:
                        return;
                    case COM_INIT_DB:
                        comInitDB(limited_payload);
                        break;
                    case COM_QUERY:
                        comQuery(payload, false);
                        break;
                    case COM_FIELD_LIST:
                        comFieldList(limited_payload);
                        break;
                    case COM_PING:
                        comPing();
                        break;
                    case COM_STMT_PREPARE:
                        comStmtPrepare(payload);
                        break;
                    case COM_STMT_EXECUTE:
                        comStmtExecute(payload);
                        break;
                    case COM_STMT_CLOSE:
                        comStmtClose(payload);
                        break;
                    default:
                        throw Exception(Poco::format("Command %d is not implemented.", command), ErrorCodes::NOT_IMPLEMENTED);
                }
            }
            catch (const NetException & exc)
            {
                log->log(exc);
                throw;
            }
            catch (...)
            {
                tryLogCurrentException(log, "MySQLHandler: Cannot read packet: ");
                packet_endpoint->sendPacket(ERRPacket(getCurrentExceptionCode(), "HY000", getCurrentExceptionMessage(false)), true);
            }
        }
    }
    catch (const Poco::Exception & exc)
    {
        log->log(exc);
    }
}

/** Reads 3 bytes, finds out whether it is SSLRequest or HandshakeResponse packet, starts secure connection, if it is SSLRequest.
 *  Reading is performed from socket instead of ReadBuffer to prevent reading part of SSL handshake.
 *  If we read it from socket, it will be impossible to start SSL connection using Poco. Size of SSLRequest packet payload is 32 bytes, thus we can read at most 36 bytes.
 */
void MySQLHandler::finishHandshake(MySQLProtocol::ConnectionPhase::HandshakeResponse & packet)
{
    size_t packet_size = PACKET_HEADER_SIZE + SSL_REQUEST_PAYLOAD_SIZE;

    /// Buffer for SSLRequest or part of HandshakeResponse.
    char buf[packet_size];
    size_t pos = 0;

    /// Reads at least count and at most packet_size bytes.
    // buf is initialized in lambda under receiveBytes
    // coverity[uninit_use]
    auto read_bytes = [this, &buf, &pos, &packet_size](size_t count) -> void {
        while (pos < count)
        {
            int ret = socket().receiveBytes(buf + pos, static_cast<uint32_t>(packet_size - pos));
            if (ret == 0)
            {
                throw Exception("Cannot read all data. Bytes read: " + std::to_string(pos) + ". Bytes expected: 3", ErrorCodes::CANNOT_READ_ALL_DATA);
            }
            pos += ret;
        }
    };
    read_bytes(3); /// We can find out whether it is SSLRequest of HandshakeResponse by first 3 bytes.

    size_t payload_size = unalignedLoad<uint32_t>(buf) & 0xFFFFFFu;
    LOG_TRACE(log, "payload size: {}", payload_size);

    if (payload_size == SSL_REQUEST_PAYLOAD_SIZE)
    {
        finishHandshakeSSL(packet_size, buf, pos, read_bytes, packet);
    }
    else
    {
        /// Reading rest of HandshakeResponse.
        packet_size = PACKET_HEADER_SIZE + payload_size;
        WriteBufferFromOwnString buf_for_handshake_response;
        buf_for_handshake_response.write(buf, pos);
        copyData(*packet_endpoint->in, buf_for_handshake_response, packet_size - pos);
        ReadBufferFromString payload(buf_for_handshake_response.str());
        payload.ignore(PACKET_HEADER_SIZE);
        packet.readPayloadWithUnpacked(payload);
        packet_endpoint->sequence_id++;
    }
}

void MySQLHandler::authenticate(const String & user_name, const String & auth_plugin_name, const String & initial_auth_response)
{
    try
    {
        // For compatibility with JavaScript MySQL client, Native41 authentication plugin is used when possible (if password is specified using double SHA1). Otherwise SHA256 plugin is used.
        auto user = connection_context->getAccessControlManager().read<User>(user_name);
        const DB::Authentication::Type user_auth_type = user->authentication.getType();
        if (user_auth_type == DB::Authentication::SHA256_PASSWORD)
        {
            authPluginSSL();
        }

        std::optional<String> auth_response = auth_plugin_name == auth_plugin->getName() ? std::make_optional<String>(initial_auth_response) : std::nullopt;
        auth_plugin->authenticate(user_name, auth_response, connection_context, packet_endpoint, secure_connection, socket().peerAddress());
    }
    catch (const Exception & exc)
    {
        LOG_ERROR(log, "Authentication for user {} failed.", user_name);
        packet_endpoint->sendPacket(ERRPacket(exc.code(), "HY000", exc.message()), true);
        throw;
    }
    LOG_DEBUG(log, "Authentication for user {} succeeded.", user_name);
}

void MySQLHandler::comInitDB(ReadBuffer & payload)
{
    String database;
    readStringUntilEOF(database, payload);
    database = formatTenantDatabaseNameWithTenantId(database, connection_context->getTenantId(), '.');
    LOG_DEBUG(log, "Setting current database to {}", database);
    connection_context->setCurrentDatabase(database);
    packet_endpoint->sendPacket(OKPacket(0, client_capabilities, 0, 0, 1), true);
}

void MySQLHandler::comFieldList(ReadBuffer & payload)
{
    ComFieldList packet;
    packet.readPayloadWithUnpacked(payload);
    String database = connection_context->getCurrentDatabase();
    StoragePtr table_ptr = DatabaseCatalog::instance().getTable({database, packet.table}, connection_context);
    auto metadata_snapshot = table_ptr->getInMemoryMetadataPtr();
    for (const NameAndTypePair & column : metadata_snapshot->getColumns().getAll())
    {
        ColumnDefinition column_definition(
            database, packet.table, packet.table, column.name, column.name, CharacterSet::binary, 100, ColumnType::MYSQL_TYPE_STRING, 0, 0, true
        );
        packet_endpoint->sendPacket(column_definition);
    }
    packet_endpoint->sendPacket(OKPacket(0xfe, client_capabilities, 0, 0, 0), true);
}

void MySQLHandler::comPing()
{
    packet_endpoint->sendPacket(OKPacket(0x0, client_capabilities, 0, 0, 0), true);
}

static bool isFederatedServerSetupSetCommand(const String & query);

void MySQLHandler::comQuery(ReadBuffer & payload, bool binary_protocol)
{
    String query = String(payload.position(), payload.buffer().end());

    /* strip comments */
    if (query.starts_with("/*"))
    {
        auto sz = query.find("*/", 2);
        if (sz != String::npos)
        {
            sz += 2;

            /* remove space */
            if (query.size() > sz && query[sz] == ' ')
                sz++;

            query = query.substr(sz);
            payload.ignore(sz);
        }
    }

    LOG_INFO(log, "MySQL server received: " + query);

    /// skip empty queries
    if (query.empty())
    {
        packet_endpoint->sendPacket(OKPacket(0x00, client_capabilities, 0, 0, 0), true);
    }
    // This is a workaround in order to support adding ClickHouse to MySQL using federated server.
    // As Clickhouse doesn't support these statements, we just send OK packet in response.
    else if (isFederatedServerSetupSetCommand(query))
    {
        packet_endpoint->sendPacket(OKPacket(0x00, client_capabilities, 0, 0, 0), true);
    }
    else
    {
        String replacement_query;
        bool should_replace = false;
        bool with_output = false;

        // Queries replacements
        for (auto const & [query_to_replace, replacement_fn] : queries_replacements)
        {
            if (0 == strncasecmp(query_to_replace.c_str(), query.c_str(), query_to_replace.size()))
            {
                should_replace = true;
                replacement_query = replacement_fn(query, query_to_replace);
                break;
            }
        }

        // Settings replacements
        if (!should_replace)
            for (auto const & [mysql_setting, native_setting] : settings_replacements)
            {
                const auto replacement_query_opt = setSettingReplacementQuery(query, mysql_setting, native_setting);
                if (replacement_query_opt.has_value())
                {
                    should_replace = true;
                    replacement_query = replacement_query_opt.value();
                    break;
                }
            }

        ReadBufferFromString replacement(replacement_query);
        auto query_context = Context::createCopy(connection_context);
        query_context->setCurrentTransaction(nullptr, false);
        query_context->setCurrentQueryId(fmt::format("mysql:{}:{}", connection_id, toString(UUIDHelpers::generateV4())));
        /// TODO(fredwang) only set it for queries from IDEs (not users)
        query_context->setSetting("text_case_option", String{"MIXED"});
        query_context->setSetting("enable_multiple_tables_for_cnch_parts", 1);
        /// if the following settings are false, IDE would get BLOB type for string columns,
        /// and then fail to parse data from disk CSV files
        query_context->setSetting("mysql_map_string_to_text_in_show_columns", 1);
        query_context->setSetting("mysql_map_fixed_string_to_text_in_show_columns", 1);
        /// TODO(fredwang) change it to a smaller threshold?
        query_context->setSetting("max_execution_time", 18000);
        /// required by quickbi, otherwise it would fail to get table info
        query_context->setSetting("allow_mysql_having_name_resolution", 1);
        /// to collect the affected rows for update/delete/insert
        query_context->setSetting("insert_select_with_profiles", 1);
        CurrentThread::QueryScope query_scope{query_context};

        std::atomic<size_t> affected_rows {0};
        auto prev = query_context->getProgressCallback();
        query_context->setProgressCallback([&, prev = prev](const Progress & progress)
        {
            if (prev)
                prev(progress);
            affected_rows += progress.written_rows;
        });

        FormatSettings format_settings;
        format_settings.mysql_wire.client_capabilities = client_capabilities;
        format_settings.mysql_wire.max_packet_size = max_packet_size;
        format_settings.mysql_wire.sequence_id = &sequence_id;
        format_settings.mysql_wire.binary_protocol = binary_protocol;

        auto set_result_details = [&with_output](const String &, const String &, const String &, const String &, MPPQueryCoordinatorPtr)
        {
            with_output = true;
        };

        executeQuery(should_replace ? replacement : payload, *out, false, query_context, set_result_details, format_settings);

        if (!with_output) {
            packet_endpoint->sendPacket(OKPacket(0x00, client_capabilities, affected_rows, 0, 0), true);
        }
    }
}

void MySQLHandler::comStmtPrepare(DB::ReadBuffer & payload)
{
    String statement;
    readStringUntilEOF(statement, payload);

    auto statement_id_opt = emplacePreparedStatement(std::move(statement));
    if (statement_id_opt.has_value())
        packet_endpoint->sendPacket(PreparedStatementResponseOK(statement_id_opt.value(), 0, 0, 0), true);
    else
        packet_endpoint->sendPacket(ERRPacket(), true);
}

void MySQLHandler::comStmtExecute(ReadBuffer & payload)
{
    uint32_t statement_id;
    payload.readStrict(reinterpret_cast<char *>(&statement_id), 4);

    auto statement_opt = getPreparedStatement(statement_id);
    if (statement_opt.has_value())
        MySQLHandler::comQuery(statement_opt.value(), true);
    else
        packet_endpoint->sendPacket(ERRPacket(), true);
};

void MySQLHandler::comStmtClose(ReadBuffer & payload)
{
    uint32_t statement_id;
    payload.readStrict(reinterpret_cast<char *>(&statement_id), 4);

    // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html
    // No response packet is sent back to the client.
    erasePreparedStatement(statement_id);
};

std::optional<UInt32> MySQLHandler::emplacePreparedStatement(String statement)
{
    static constexpr size_t MAX_PREPARED_STATEMENTS = 10'000;
    std::lock_guard<std::mutex> lock(prepared_statements_mutex);
    if (prepared_statements.size() > MAX_PREPARED_STATEMENTS) /// Shouldn't happen in reality as COM_STMT_CLOSE cleans up the elements
    {
        LOG_ERROR(log, "Too many prepared statements");
        current_prepared_statement_id = 0;
        prepared_statements.clear();
        return {};
    }

    uint32_t statement_id = current_prepared_statement_id;
    ++current_prepared_statement_id;

    // Key collisions should not happen here, as we remove the elements from the map with COM_STMT_CLOSE,
    // and we have quite a big range of available identifiers with 32-bit unsigned integer
    if (prepared_statements.contains(statement_id))
    {
        LOG_ERROR(
            log,
            "Failed to store a new statement `{}` with id {}; it is already taken by `{}`",
            statement,
            statement_id,
            prepared_statements.at(statement_id));
        return {};
    }

    prepared_statements.emplace(statement_id, statement);
    return std::make_optional(statement_id);
};

std::optional<ReadBufferFromString> MySQLHandler::getPreparedStatement(UInt32 statement_id)
{
    std::lock_guard<std::mutex> lock(prepared_statements_mutex);
    if (!prepared_statements.contains(statement_id))
    {
        LOG_ERROR(log, "Could not find prepared statement with id {}", statement_id);
        return {};
    }
    // Temporary workaround as we work only with queries that do not bind any parameters atm
    return std::make_optional<ReadBufferFromString>(prepared_statements.at(statement_id));
}

void MySQLHandler::erasePreparedStatement(UInt32 statement_id)
{
    std::lock_guard<std::mutex> lock(prepared_statements_mutex);
    prepared_statements.erase(statement_id);
}
void MySQLHandler::authPluginSSL()
{
    throw Exception("ClickHouse was built without SSL support. Try specifying password using double SHA1 in users.xml.", ErrorCodes::SUPPORT_IS_DISABLED);
}

void MySQLHandler::finishHandshakeSSL(
    [[maybe_unused]] size_t packet_size, [[maybe_unused]] char * buf, [[maybe_unused]] size_t pos,
    [[maybe_unused]] std::function<void(size_t)> read_bytes, [[maybe_unused]] MySQLProtocol::ConnectionPhase::HandshakeResponse & packet)
{
    throw Exception("Client requested SSL, while it is disabled.", ErrorCodes::SUPPORT_IS_DISABLED);
}

#if USE_SSL
MySQLHandlerSSL::MySQLHandlerSSL(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, bool ssl_enabled, uint32_t connection_id_, RSA & public_key_, RSA & private_key_)
    : MySQLHandler(server_, tcp_server_, socket_, ssl_enabled, connection_id_)
    , public_key(public_key_)
    , private_key(private_key_)
{}

void MySQLHandlerSSL::authPluginSSL()
{
    auth_plugin = std::make_unique<MySQLProtocol::Authentication::Sha256Password>(public_key, private_key, log->name());
}

void MySQLHandlerSSL::finishHandshakeSSL(
    size_t packet_size, char *buf, size_t pos, std::function<void(size_t)> read_bytes,
    MySQLProtocol::ConnectionPhase::HandshakeResponse & packet)
{
    read_bytes(packet_size); /// Reading rest SSLRequest.
    SSLRequest ssl_request;
    ReadBufferFromMemory payload(buf, pos);
    payload.ignore(PACKET_HEADER_SIZE);
    ssl_request.readPayloadWithUnpacked(payload);
    client_capabilities = ssl_request.capability_flags;
    // capability_flags initialized under readPayloadWithUnpacked
    // coverity[uninit_use]
    max_packet_size = ssl_request.max_packet_size ? ssl_request.max_packet_size : MAX_PACKET_LENGTH;
    secure_connection = true;
    ss = std::make_shared<SecureStreamSocket>(SecureStreamSocket::attach(socket(), SSLManager::instance().defaultServerContext()));
    in = std::make_shared<ReadBufferFromPocoSocket>(*ss);
    out = std::make_shared<WriteBufferFromPocoSocket>(*ss);
    sequence_id = 2;
    packet_endpoint = std::make_shared<MySQLProtocol::PacketEndpoint>(*in, *out, sequence_id);
    packet_endpoint->receivePacket(packet); /// Reading HandshakeResponse from secure socket.
}

#endif

static bool isFederatedServerSetupSetCommand(const String & query)
{
    re2::RE2::Options regexp_options;
    regexp_options.set_case_sensitive(false);
    static const re2::RE2 expr(
        "(^(SET NAMES(.*)))"
        "|(^(SET character_set_results(.*)))"
        "|(^(SET (GLOBAL )?FOREIGN_KEY_CHECKS(.*)))"
        "|(^(SET AUTOCOMMIT(.*)))"
        "|(^(SET sql_mode(.*)))"
        "|(^(SET @@(.*)))"
        "|(^(SET UNIQUE_CHECKS(.*)))"
        "|(^(SET SESSION TRANSACTION ISOLATION LEVEL(.*)))", regexp_options);
    assert(expr.ok());
    return re2::RE2::FullMatch(query, expr);
}

/// Replace "[query(such as SHOW VARIABLES...)]" into "".
static String selectEmptyReplacementQuery(const String & /*query*/, const String & /*prefix*/)
{
    return "select ''";
}

static String selectEmptySetQuery(const String & /*query*/, const String & /*prefix*/)
{
    return "select * from information_schema.events where 0 = 1";
}

/// "SHOW TABLE STATUS [{FROM | IN} db_name] [LIKE 'patten' | WHERE expr]
/// by joining cnch_tables and cnch_parts on table name to get the table status
static String showTableStatusReplacementQuery(const String & query, const String & prefix)
{
    String select = "SELECT T.name as Name, any(engine) as Engine, NULL as Version, NULL as Row_format, ifNull(sum(rows_count), 0) as Rows, ifNull(floor(divide(Data_length, sum(rows_count))), 0) as Avg_row_length, ifNull(sum(bytes_on_disk), 0) as Data_length, 0 AS Max_data_length, 0 AS Index_length, 0 AS Data_free, NULL AS Auto_increment, '2024-01-01 00:00:01' AS Create_time, max(modification_time) AS Update_time, NULL AS Check_time, 'utf8mb4_0900_ai_ci' AS Collation, NULL AS Checksum, '' AS Create_options, '' AS Comment FROM system.cnch_tables as T LEFT OUTER JOIN (SELECT * FROM system.cnch_parts WHERE visible = 1) as P on T.database = P.database and T.name = P.table ";

    if (query.size() > prefix.size())
    {
        std::regex regexPattern(prefix + R"((?: (?:FROM|IN) (\x60\w+\x60|[^ ]+))?(?: LIKE (.+)| WHERE (.+))?)", std::regex_constants::icase);

        std::smatch match;
        if (std::regex_search(query, match, regexPattern))
        {
            String dbName = match[1].str();
            if (match[1].matched)
            {
                if (dbName.size() > 2 and dbName.front() == '`' and dbName.back() == '`')
                    dbName.front() = dbName.back() = '\'';
                else
                    dbName = '\'' + dbName + '\'';
            }
            else
            {
                dbName = "currentDatabase(1)";
            }

            std::optional<String> tableName;
            if (match[2].matched)
                tableName.emplace(match[2].str());

            std::optional<String> where;
            if (match[3].matched)
                where.emplace(match[3].str());

            String rewritten_query = select + " WHERE T.database=" + dbName;

            if (tableName)
                rewritten_query += " AND T.name LIKE " + *tableName;

            if (where)
                rewritten_query += " AND " + *where;

            rewritten_query += " GROUP BY T.name settings enable_multiple_tables_for_cnch_parts=1";
            return rewritten_query;
        }
    }
    else if (query.size() == prefix.size())
    {
        return select + " WHERE T.database=currentDatabase(1) GROUP BY T.name settings enable_multiple_tables_for_cnch_parts=1";
    }
    return query;
}

/// For "SHOW {INDEX | INDEXES | KEYS} {FROM | IN} tbl_name [{FROM | IN} db_name] [WHERE expr]
static String showIndexReplacementQuery(const String & query, const String & prefix)
{
    if (query.size() > prefix.size())
    {
        std::regex regexPattern(prefix + R"( (?:FROM|IN) (\x60\w+\x60|[^ ]+)(?:\.(\x60\w+\x60|[^ ]+))?(?: (?:FROM|IN) (\x60\w+\x60|[^ ]+))?(?: WHERE (.+))?)", std::regex_constants::icase);

        std::smatch match;
        if (std::regex_search(query, match, regexPattern))
        {
            int dbId = 0;
            int tableId = 0;

            /// SHOW INDEX FROM sakila.actor
            if (match[2].matched)
            {
                dbId = 1;
                tableId = 2;
            }
            /// SHOW INDEX FROM actor FROM sakila
            else if (match[3].matched)
            {
                dbId = 3;
                tableId = 1;
            }
            /// SHOW INDEX FROM actor
            else
            {
                tableId = 1;
            }

            String dbName =  match[dbId].str();

            if (dbId == 0)
                dbName = "currentDatabase(1)" ;
            else if (dbName.size() > 2 and dbName.front() == '`' and dbName.back() == '`')
                dbName.front() = dbName.back() = '\'';
            else
                dbName = '\'' + dbName + '\'';

            String tableName = match[tableId].str();
            if (tableName.size() > 2 and tableName.front() == '`' and tableName.back() == '`')
                tableName.front() = tableName.back() = '\'';
            else
                tableName = '\'' + tableName + '\'';

            return "SELECT table_name as Table, non_unique as Non_unique, index_name as Key_name, "
                "seq_in_index as Seq_in_index, column_name as Column_name, collation as Collation, "
                "cardinality as Cardinality, sub_part as Sub_part, packed as Packed, nullable as Null, "
                "index_type as Index_type, comment as Comment, index_comment as Index_comment, "
                "is_visible as Visible, expression as Expression "
                "FROM information_schema.statistics "
                "WHERE table_schema=" + dbName + " AND table_name = " + tableName + (match[4].matched ? " AND " + match[4].str() : "");
        }
    }
    return query;
}

static std::optional<String> setSettingReplacementQuery(const String & query, const String & mysql_setting, const String & native_setting)
{
    const String prefix = "SET " + mysql_setting;
    if (0 == strncasecmp(prefix.c_str(), query.c_str(), prefix.size()))
        return "SET " + native_setting + String(query.data() + prefix.length());
    return std::nullopt;
}

/// Replace "KILL QUERY [connection_id]" into "KILL QUERY WHERE query_id LIKE 'mysql:[connection_id]:xxx'".
static String killConnectionIdReplacementQuery(const String & query, const String & prefix)
{
    if (query.size() > prefix.size())
    {
        String suffix = query.data() + prefix.length();
        static const re2::RE2 expr("^[0-9]");
        if (re2::RE2::FullMatch(suffix, expr))
        {
            String replacement = fmt::format("KILL QUERY WHERE query_id LIKE 'mysql:{}:%'", suffix);
            return replacement;
        }
    }
    return query;
}

}
