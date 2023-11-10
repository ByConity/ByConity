#include <Processors/tests/gtest_exchange_helper.h>

brpc::Server ExchangeRemoteTest::server;
Coordination::BrpcExchangeReceiverRegistryService ExchangeRemoteTest::service_impl(73400320);
