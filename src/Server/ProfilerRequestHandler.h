#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>


namespace DB
{

class IServer;

/// Response with a static file for requests from a browser.
class ProfilerRequestHandler : public HTTPRequestHandler 
{
private:
    IServer & server;

public:
    explicit ProfilerRequestHandler(IServer & server);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};

}
