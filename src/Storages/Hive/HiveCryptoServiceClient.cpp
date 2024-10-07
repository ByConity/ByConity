#include <mutex>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <Storages/Hive/HiveCryptoServiceClient.h>
#include <Storages/Hive/ZtiSdk.h>
#include <Poco/Base64Decoder.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/MemoryStream.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <common/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
    extern const int UNKNOWN_EXCEPTION;
    extern const int CRYPTO_SERVICE_ERROR;
}

static String base64Decode(const String & encoded)
{
    if (encoded.empty())
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "base64 decode failed: {}", encoded);
    String decoded;
    Poco::MemoryInputStream istr(encoded.data(), encoded.size());
    Poco::Base64Decoder decoder(istr);
    Poco::StreamCopier::copyToString(decoder, decoded);
    return decoded;
}

HiveCryptoServiceClient & HiveCryptoServiceClient::instance()
{
    static HiveCryptoServiceClient instance;
    return instance;
}

HiveCryptoServiceClient::HiveCryptoServiceClient() : max_concurrency(1024), use_cache(true), encryption_key_cache(65536)
{
}

void HiveCryptoServiceClient::init(const Poco::Util::AbstractConfiguration & config)
{
    if (config.has("hive_crypto_configuration.max_concurrency"))
    {
        const_cast<int64_t &>(max_concurrency) = config.getInt64("hive_crypto_configuration.max_concurrency");
    }
    if (config.has("hive_crypto_configuration.use_cache"))
    {
        const_cast<bool &>(use_cache) = config.getBool("hive_crypto_configuration.use_cache");
    }
    if (config.has("hive_crypto_configuration.cache_size"))
    {
        encryption_key_cache.setCapacity(config.getUInt64("hive_crypto_configuration.cache_size"));
    }
}

size_t curlWriteMemoryCallback(void * contents, size_t size, size_t nmemb, void * userp)
{
    auto * buffer = static_cast<std::string *>(userp);
    buffer->append(static_cast<char *>(contents), size * nmemb);
    return size * nmemb;
}

void HiveCryptoServiceClient::pocoPostJson(const String & url, const String & request_body, String & response_body)
{
    SCOPE_EXIT({ --num_in_flight_request; });
    const auto concurrency = ++num_in_flight_request;
    if (concurrency > max_concurrency)
        throw Exception(ErrorCodes::CRYPTO_SERVICE_ERROR, "too many in-flight hive crypto requests");
    const auto start = Clock::now();
    const Poco::URI uri(url);
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
    request.setHost(uri.getHost());
    request.setContentType("application/json");
    request.setContentLength(request_body.size());

    ConnectionTimeouts timeouts({10, 0}, {10, 0}, {10, 0});
    auto session = makeHTTPSession(uri, timeouts);
    session->setKeepAlive(true);
    std::ostream & request_body_os = session->sendRequest(request);
    request_body_os << request_body;

    Poco::Net::HTTPResponse response;
    auto & response_body_is = session->receiveResponse(response);
    response_body.clear();
    Poco::StreamCopier::copyToString(response_body_is, response_body);

    LOG_TRACE(
        log,
        "pocoPostJson successfully, url: {}, response status: {}, time: {}ms, concurrency: {}/{}",
        url,
        response.getStatus(),
        std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count(),
        concurrency,
        max_concurrency);
}

String HiveCryptoServiceClient::unwrapKey(const String & url_prefix, const String & key_identifier, const String & wrapped_key)
{
    const auto start = Clock::now();
    if (!use_cache)
    {
        String plain_key = unwrapKeyImpl(url_prefix, key_identifier, wrapped_key);
        LOG_TRACE(
            log,
            "unwrapKey successfully without cache, key_identifier: {}, wrapped_key: {}, time: {}ms",
            key_identifier,
            wrapped_key,
            std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count());
        return plain_key;
    }
    else
    {
        auto loader = [&] { return std::make_shared<String>(unwrapKeyImpl(url_prefix, key_identifier, wrapped_key)); };
        auto kv = encryption_key_cache.getOrSet(url_prefix + key_identifier, loader);
        LOG_TRACE(
            log,
            "unwrapKey successfully with cache, key_identifier: {}, wrapped_key: {}, cache_hit: {}, cache_size: {}, time: {}ms",
            key_identifier,
            wrapped_key,
            !kv.second,
            encryption_key_cache.getCapacity(),
            std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count());
        return *kv.first;
    }
}

String HiveCryptoServiceClient::unwrapKeyImpl(const String & url_prefix, const String & key_identifier, const String & wrapped_key)
{
    // Request/response format: https://bytedance.us.larkoffice.com/docx/NBIidl6QUot6cyxnd2wusoo9sUf
    const String url(url_prefix + "/unwrapKey");

    // Build post body
    Poco::JSON::Object body_json;
    Poco::JSON::Object access_token;
    access_token.set("tokenType", "zti");
    String zti_token;
    zti::ZtiSDK::GetInstance().GetToken(zti_token);
    access_token.set("tokenContent", zti_token);
    body_json.set("accessToken", access_token);
    body_json.set("wrappedKey", wrapped_key);
    body_json.set("masterkeyIdentifier", key_identifier);
    std::stringstream request_body_ss;
    body_json.stringify(request_body_ss);
    const String request_body = request_body_ss.str();

    Int32 retry_time = 0;
    while (retry_time++ < 3)
    {
        try
        {
            String response_body;
            pocoPostJson(url, request_body, response_body);

            Poco::JSON::Parser parser;
            Poco::Dynamic::Var parse_result = parser.parse(response_body);
            const auto & json_body = parse_result.extract<Poco::JSON::Object::Ptr>();

            if (!json_body || !json_body->has("code") || !json_body->has("msg"))
                throw Exception(ErrorCodes::UNKNOWN_FORMAT, "failed to parse json body, response_body: {}", response_body);

            const auto code = json_body->getValue<Int32>("code");
            const auto msg = json_body->getValue<String>("msg");
            if (code != 0)
                throw Exception(ErrorCodes::CRYPTO_SERVICE_ERROR, "crypto service error, msg: {}", msg);
            if (!json_body->has("data"))
                throw Exception(ErrorCodes::UNKNOWN_FORMAT, "failed to parse json body, response_body: {}", response_body);

            return base64Decode(json_body->getValue<String>("data"));
        }
        catch (const Exception & e)
        {
            throw e;
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }

    throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "failed to call crypto service");
}
}
