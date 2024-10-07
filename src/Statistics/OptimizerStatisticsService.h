
#pragma once

#include <Common/Logger.h>
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

    void fetchStatisticsSettings(
        google::protobuf::RpcController * cntl,
        const Protos::FetchStatisticsSettingsRequest * request,
        Protos::FetchStatisticsSettingsResponse * response,
        google::protobuf::Closure * done) override;

private:
    LoggerPtr log = getLogger("OptimizerStatisticsService");
};


}

namespace DB {
using Statistics::OptimizerStatisticsService;
REGISTER_SERVICE_IMPL(OptimizerStatisticsService);
}
