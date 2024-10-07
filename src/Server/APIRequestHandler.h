#pragma once

#include <Common/Logger.h>
#include <Server/HTTP/HTMLForm.h>
#include <Common/StringUtils/StringUtils.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <rapidjson/document.h>
#include <common/logger_useful.h>
#include <Server/IServer.h>
#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{
class APIRequestHandler : public HTTPRequestHandler, WithMutableContext
{
public:
    struct Result
    {
        explicit Result(int code = 0) : doc(rapidjson::kObjectType)
        {
            if (code)
                doc.AddMember("status", "FAIL", doc.GetAllocator());
            else
                doc.AddMember("status", "OK", doc.GetAllocator());
            doc.AddMember("code", code, doc.GetAllocator());
        }


        template <class K, class V>
        void add(K && k, V && v)
        {
            doc.AddMember(std::forward<K>(k), std::forward<V>(v), doc.GetAllocator());
        }

        template <class K>
        void add(K && k, const std::string & v)
        {
            doc.AddMember(std::forward<K>(k), rapidjson::StringRef(v.c_str()), doc.GetAllocator());
        }

        rapidjson::Document doc;
    };

    explicit APIRequestHandler(IServer & server_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    void sendResult(const Result & res, HTTPServerResponse &);
    void trySendExceptionToClient(const std::string & exception_msg, int exception_code, HTTPServerRequest &, HTTPServerResponse &);

    void onResourceReportAction(HTTPServerRequest &, HTMLForm & params, HTTPServerResponse &);

    LoggerPtr log;
    std::string server_display_name;
};

}
