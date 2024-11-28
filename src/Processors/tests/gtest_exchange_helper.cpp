#include <Processors/tests/gtest_exchange_helper.h>
#include <incubator-brpc/src/brpc/server.h>

brpc::Server * ExchangeRemoteTest::server = new brpc::Server;
Coordination::BrpcExchangeReceiverRegistryService * ExchangeRemoteTest::service_impl
    = new Coordination::BrpcExchangeReceiverRegistryService();

DB::HostWithPortsVec MockServiceDiscoveryClient::lookup(const String & /*psm_name*/, DB::ComponentType /*type*/, const String & /*vw_name*/)
{
    return {DB::HostWithPorts("127.0.0.1", brpc_server_port, 0, 0, 0, 0, "")};
}
