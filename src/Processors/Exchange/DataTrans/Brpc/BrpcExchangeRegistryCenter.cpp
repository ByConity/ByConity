#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeRegistryCenter.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastSender.h>


namespace DB {
BroadcastSenderPtr
BrpcExchangeRegistryCenter::getOrCreateSender(std::vector<DataTransKeyPtr> trans_keys, ContextPtr context, Block header)
{
    return std::dynamic_pointer_cast<IBroadcastSender>(
        std::make_shared<BrpcRemoteBroadcastSender>(std::move(trans_keys), std::move(context), std::move(header)));
}

BroadcastReceiverPtr BrpcExchangeRegistryCenter::getOrCreateReceiver(
    DataTransKeyPtr trans_key, String registry_address, ContextPtr context, Block header)
{
    return std::dynamic_pointer_cast<IBroadcastReceiver>(std::make_shared<BrpcRemoteBroadcastReceiver>(
        std::move(trans_key), std::move(registry_address), std::move(context), std::move(header)));
}
}
