#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <common/logger_useful.h>


namespace DB
{

class LibraryBridgeHandlerFactory : public HTTPRequestHandlerFactory, WithContext
{
public:
    LibraryBridgeHandlerFactory(
        const std::string & name_,
        size_t keep_alive_timeout_,
        ContextPtr context_);

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    LoggerPtr log;
    const std::string name;
    const size_t keep_alive_timeout;
};

}
