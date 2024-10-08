#include "ProfilerRequestHandler.h"
#include "IServer.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <IO/HTTPCommon.h>
#include <common/getResource.h>
#include <common/logger_useful.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB {

ProfilerRequestHandler::ProfilerRequestHandler(IServer & server_): server(server_)
{
}

void extractResourceAndType(const std::string & uri, std::string & resource, std::string & type)
{
    std::string extension = fs::path(uri).extension();
    std::transform(extension.begin(), extension.end(), extension.begin(), [](unsigned char c) { return std::tolower(c); });

    resource = uri.substr(1);

    if (extension == ".png")
    {
        type = "image/png";
    }
    else if(extension == ".jpg" || extension == ".jpeg")
    {
        type = "image/jp2";
    }
    else if (extension == ".svg")
    {
        type = "image/svg+xml";
    }
    else if (extension == ".js")
    {
        type = "text/javascript";
    }
    else if (extension == ".css" || extension == ".html") 
    {
        type = "text/" + extension.substr(1);
    }
    else if (extension.empty())
    {
        type = "text/html";
        resource = "profiler/index.html";
    }
    else 
    {
        type = "text/html";
    }
    type = type + "; charset=UTF-8";
}

void ProfilerRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
   
    const auto & uri = request.getURI();
    LOG_INFO(getLogger("ProfilerHttp"), "fetching {}", uri);

    std::string resource_name, content_type;
    extractResourceAndType(uri, resource_name, content_type);
    response.setContentType(content_type);

    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    auto keep_alive_timeout = server.config().getUInt("keep_alive_timeout", 10);
    setResponseDefaultHeaders(response, keep_alive_timeout);
    const auto content = getResource(resource_name);
    if (content.empty())
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
    else
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    *response.send() << content;

    LOG_INFO(getLogger("ProfilerHttp"), "return {}, type {}", resource_name, content_type);
}

}
