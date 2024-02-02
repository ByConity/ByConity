#pragma once

#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <unordered_map>

namespace DB
{
    using HttpHeaders = std::unordered_map<String, String>;

    /**
     * Perform HTTP request.
     * Note: Chunked transfer encoding is false.
     */
    class HTTPSender
    {
    private:
        HTTPSessionPtr session;
        Poco::Net::HTTPRequest request;
        Poco::Net::HTTPResponse response;

    public:
        explicit HTTPSender(const Poco::URI &uri,
                            const std::string &method = Poco::Net::HTTPRequest::HTTP_POST,
                            const ConnectionTimeouts &timeouts = {},
                            const HttpHeaders &httpHeaders = {}
        );

        /// you can set request body for POST/PUT method
        void send(const std::string & body = {});

        /// Receives response from the server after sending all data.
        std::istream * handleResponse();
    };
}
