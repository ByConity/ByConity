#include <IO/HTTPSender.h>
#include <common/logger_useful.h>


namespace DB
{
HTTPSender::HTTPSender(
    const Poco::URI & uri, const std::string & method, const ConnectionTimeouts & timeouts, const HttpHeaders & httpHeaders)
    : session{makeHTTPSession(uri, timeouts)}, request{method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1}
{
    for (const auto & header : httpHeaders)
    {
        request.set(header.first, header.second);
    }

    request.setHost(uri.getHost());
    request.setChunkedTransferEncoding(false);
}

void HTTPSender::send(const std::string & body)
{
    LOG_TRACE((getLogger("HTTPSender")), "Sending request to {}", request.getURI());

    if (body.empty())
    {
        session->sendRequest(request);
    }
    else
    {
        request.setContentLength(body.size());
        std::ostream & os = session->sendRequest(request);
        os << body;
    }
}

std::istream * HTTPSender::handleResponse()
{
    return receiveResponse(*session, request, response, true);
}
}
