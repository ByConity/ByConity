#include "APIRequestHandler.h"


#include <chrono>
#include <iomanip>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/ExternalTable.h>
#include <DataStreams/IBlockInputStream.h>
#include <IO/BrotliReadBuffer.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <IO/copyData.h>
#include <Interpreters/executeQuery.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerRequestImpl.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPStream.h>
#include <Poco/Net/NetException.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <Common/CurrentThread.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/config.h>
#include <Common/escapeForFileName.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int READONLY;
    extern const int UNKNOWN_COMPRESSION_METHOD;

    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_OPEN_FILE;

    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int TOO_DEEP_AST;
    extern const int TOO_BIG_AST;
    extern const int UNEXPECTED_AST_STRUCTURE;

    extern const int SYNTAX_ERROR;

    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_FUNCTION;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_TYPE;
    extern const int UNKNOWN_STORAGE;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_SETTING;
    extern const int UNKNOWN_DIRECTION_OF_SORTING;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int UNKNOWN_FORMAT;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int UNKNOWN_TYPE_OF_QUERY;

    extern const int QUERY_IS_TOO_LARGE;

    extern const int NOT_IMPLEMENTED;
    extern const int SOCKET_TIMEOUT;

    extern const int UNKNOWN_USER;
    extern const int WRONG_PASSWORD;
    extern const int REQUIRED_PASSWORD;

    extern const int INVALID_SESSION_TIMEOUT;
    extern const int HTTP_LENGTH_REQUIRED;
    extern const int NO_AVAILABLE_WORKER;
}

APIRequestHandler::APIRequestHandler(IServer & server)
    : WithMutableContext(server.context()), log(getLogger("HTTPHandler for API"))
{
    server_display_name = getContext()->getConfigRef().getString("display_name", getFQDNOrHostName());
}

static Poco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code)
{
    using namespace Poco::Net;

    if (exception_code == ErrorCodes::REQUIRED_PASSWORD)
        return HTTPResponse::HTTP_UNAUTHORIZED;
    else if (
        exception_code == ErrorCodes::CANNOT_PARSE_TEXT || exception_code == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE
        || exception_code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING || exception_code == ErrorCodes::CANNOT_PARSE_DATE
        || exception_code == ErrorCodes::CANNOT_PARSE_DATETIME || exception_code == ErrorCodes::CANNOT_PARSE_NUMBER)
        return HTTPResponse::HTTP_BAD_REQUEST;
    else if (
        exception_code == ErrorCodes::UNKNOWN_ELEMENT_IN_AST || exception_code == ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE
        || exception_code == ErrorCodes::TOO_DEEP_AST || exception_code == ErrorCodes::TOO_BIG_AST
        || exception_code == ErrorCodes::UNEXPECTED_AST_STRUCTURE)
        return HTTPResponse::HTTP_BAD_REQUEST;
    else if (exception_code == ErrorCodes::SYNTAX_ERROR)
        return HTTPResponse::HTTP_BAD_REQUEST;
    else if (
        exception_code == ErrorCodes::UNKNOWN_TABLE || exception_code == ErrorCodes::UNKNOWN_FUNCTION
        || exception_code == ErrorCodes::UNKNOWN_IDENTIFIER || exception_code == ErrorCodes::UNKNOWN_TYPE
        || exception_code == ErrorCodes::UNKNOWN_STORAGE || exception_code == ErrorCodes::UNKNOWN_DATABASE
        || exception_code == ErrorCodes::UNKNOWN_SETTING || exception_code == ErrorCodes::UNKNOWN_DIRECTION_OF_SORTING
        || exception_code == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION || exception_code == ErrorCodes::UNKNOWN_FORMAT
        || exception_code == ErrorCodes::UNKNOWN_DATABASE_ENGINE)
        return HTTPResponse::HTTP_NOT_FOUND;
    else if (exception_code == ErrorCodes::UNKNOWN_TYPE_OF_QUERY)
        return HTTPResponse::HTTP_NOT_FOUND;
    else if (exception_code == ErrorCodes::QUERY_IS_TOO_LARGE)
        return HTTPResponse::HTTP_REQUESTENTITYTOOLARGE;
    else if (exception_code == ErrorCodes::NOT_IMPLEMENTED)
        return HTTPResponse::HTTP_NOT_IMPLEMENTED;
    else if (exception_code == ErrorCodes::SOCKET_TIMEOUT || exception_code == ErrorCodes::CANNOT_OPEN_FILE)
        return HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
    else if (exception_code == ErrorCodes::HTTP_LENGTH_REQUIRED)
        return HTTPResponse::HTTP_LENGTH_REQUIRED;

    return HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
}

void APIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    setThreadName("APIHandler");
    ThreadStatus thread_status;

    bool with_stacktrace = false;

    try
    {
        const auto & uri = request.getURI();
        LOG_TRACE(log, "Request URI: {}", request.getURI());

        response.setContentType("application/json; charset=utf-8");
        response.set("X-ClickHouse-Server-Display-Name", server_display_name);

        /// For keep-alive to work.
        if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        HTMLForm params(getContext()->getSettingsRef(), request);
        with_stacktrace = params.getParsed<bool>("stacktrace", false);

        if (startsWith(uri, "/api/resource_report"))
        {
            onResourceReportAction(request, params, response);
        }
        else
        {
            throw Exception("No such API", ErrorCodes::BAD_ARGUMENTS);
        }

        LOG_DEBUG(log, "Done processing API request");
    }
    catch (...)
    {
        tryLogCurrentException(log);

        /** If exception is received from remote server, then stack trace is embedded in message.
          * If exception is thrown on local server, then stack trace is in separate field.
          */
        std::string exception_message = getCurrentExceptionMessage(with_stacktrace, true);
        int exception_code = getCurrentExceptionCode();

        try
        {
            trySendExceptionToClient(exception_message, exception_code, request, response);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot send exception to client");
        }
    }
}

void APIRequestHandler::sendResult(const Result & res, HTTPServerResponse & response)
{
    using namespace rapidjson;

    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    res.doc.Accept(writer);
    const auto & str = buffer.GetString();
    LOG_TRACE(log, "Response JSON: {}", str);
    *response.send() << str << std::endl;
}

void APIRequestHandler::trySendExceptionToClient(
    const std::string & exception_msg, int exception_code, [[maybe_unused]] HTTPServerRequest & request, HTTPServerResponse & response)
{
    bool auth_fail = exception_code == ErrorCodes::UNKNOWN_USER //
        || exception_code == ErrorCodes::WRONG_PASSWORD //
        || exception_code == ErrorCodes::REQUIRED_PASSWORD;

    if (auth_fail)
    {
        response.requireAuthentication("ClickHouse server HTTP API");
    }
    else
    {
        response.setStatusAndReason(exceptionCodeToHTTPStatus(exception_code));
    }

    if (!response.sent())
    {
        Result res(exception_code);
        res.add("exception", exception_msg);
        sendResult(res, response);
    }
    else
    {
        LOG_ERROR(log, "Ocurrs error while sending data due to {}", exception_msg);
    }
}

void APIRequestHandler::onResourceReportAction(
    [[maybe_unused]] HTTPServerRequest & request, [[maybe_unused]] HTMLForm & params, HTTPServerResponse & response)
{
    if (getContext()->getServerType() != ServerType::cnch_worker)
        throw Exception("/api/resource_report can only run on cnch_worker", ErrorCodes::LOGICAL_ERROR);
    if (!params.has("action"))
        throw Exception("/api/resource_report requires parameter: action", ErrorCodes::BAD_ARGUMENTS);
    auto action = params.get("action");

    Result res;
    try
    {
        if (action == "stop")
        {
            getContext()->stopResourceReport();
            res.add("success", true);
        }
        else if (action == "start")
        {
            getContext()->startResourceReport();
            res.add("success", true);
        }
        else if (action == "check")
        {
            bool registered = getContext()->isResourceReportRegistered();
            res.add("success", true);
            res.add("registered", registered);
        }
    }
    catch (const Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        res.add("success", false);
        res.add("exception", e.message());
    }
    sendResult(res, response);
}

HTTPRequestHandlerFactoryPtr createAPIRequestHandlerFactory(IServer & server, const std::string & config_prefix)
{
    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<APIRequestHandler>>(server);
    factory->addFiltersFromConfig(server.config(), config_prefix);
    return factory;
}

}
