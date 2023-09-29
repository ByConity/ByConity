#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/PrometheusMetricsWriter.h>
#include <Server/IPrometheusMetricsWriter.h>

namespace DB
{

class IServer;

class PrometheusRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
    std::shared_ptr<IPrometheusMetricsWriter> metrics_writer;

public:
    explicit PrometheusRequestHandler(IServer & server_, const std::shared_ptr<IPrometheusMetricsWriter> metrics_writer_)
        : server(server_)
        , metrics_writer(metrics_writer_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};

}
