
#pragma once

#include <Interpreters/Context_fwd.h>
#include <Protos/optimizer_statistics.pb.h>
#include <brpc/server.h>
#include <brpc/stream.h>
#include <Poco/Logger.h>
#include <Common/Brpc/BrpcServiceDefines.h>


namespace DB::Statistics
{

class OptimizerStatisticsService : public Protos::OptimizerStatisticsService, WithContext
{
public:
    explicit OptimizerStatisticsService(ContextPtr global_context) : WithContext(global_context) { }

    void refreshStatisticsCache(
        google::protobuf::RpcController * cntl,
        const Protos::RefreshStatisticsCacheRequest * request,
        Protos::RefreshStatisticsCacheResponse * response,
        google::protobuf::Closure * done) override;
#if 0
    void fetchStatisticsSettings(
        google::protobuf::RpcController * cntl,
        const Protos::FetchStatisticsSettingsRequest * request,
        Protos::FetchStatisticsSettingsResponse * response,
        google::protobuf::Closure * done) override;

    void queryUdiCounter(
        google::protobuf::RpcController * cntl,
        const Protos::QueryUdiCounterRequest * request,
        Protos::QueryUdiCounterResponse * response,
        google::protobuf::Closure * done) override;

    void ResetSQLBindingCache(
        google::protobuf::RpcController * cntl,
        const Protos::ResetSQLBindingCacheRequest * request,
        Protos::ResetSQLBindingCacheResponse * response,
        google::protobuf::Closure * done) override;
#endif

private:
    Poco::Logger * log = &Poco::Logger::get("OptimizerStatisticsService");
};


}

namespace DB {
using Statistics::OptimizerStatisticsService;
REGISTER_SERVICE_IMPL(OptimizerStatisticsService);
}
