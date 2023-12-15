#pragma once

#include <Storages/DistributedDataService.h>
#include <butil/compiler_specific.h>

namespace DB
{

class RemoteDiskCacheService : public DistributedDataService
{
public:
    explicit RemoteDiskCacheService(ContextMutablePtr & context_) : DistributedDataService(context_) { }

    explicit RemoteDiskCacheService(int max_buf_size_ = 1024 * 1024) : DistributedDataService(max_buf_size_) { }

    String getFileFullPath(const String & key) override;

    void writeRemoteFile(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::writeFileRquest * request,
        ::DB::Protos::writeFileResponse * response,
        ::google::protobuf::Closure * closeure) override;
};

REGISTER_SERVICE_IMPL(RemoteDiskCacheService);

}
