#include <CloudServices/CnchServerClient.h>
#include <Protos/RPCHelpers.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Statistics/AutoStatisticsRpcUtils.h>
#include <Common/RpcClientPool.h>
#include "Statistics/StatsTableIdentifier.h"
#include <Statistics/AutoStatsTaskLogHelper.h>

namespace DB::ErrorCodes
{
extern const int TABLE_IS_DROPPED;
}

namespace DB::Statistics::AutoStats
{

std::optional<HostWithPorts> getRemoteTargetServerIfHas(ContextPtr context, const IStorage * table)
{
    // same logic from Catalog::writeParts
    const auto host_port = context->getCnchTopologyMaster()->getTargetServer(
        UUIDHelpers::UUIDToString(table->getStorageUUID()), table->getServerVwName(), true);

    if (!host_port.empty() && !isLocalServer(host_port.getRPCAddress(), std::to_string(context->getRPCPort())))
    {
        return host_port;
    }
    else
    {
        return std::nullopt;
    }
}

void convertToProto(const TaskInfoCore & core, Protos::AutoStats::TaskInfoCore & proto)
{
    RPCHelpers::fillUUID(core.task_uuid, *proto.mutable_task_uuid());
    proto.set_task_type(core.task_type);
    RPCHelpers::fillStorageID(core.table.getStorageID(), *proto.mutable_storage_id());

    for (const auto & column_name : core.columns_name)
        proto.add_columns_name(column_name);

    proto.set_settings_json(core.settings_json);
    proto.set_stats_row_count(core.stats_row_count);
    proto.set_udi_count(core.udi_count);
    proto.set_priority(core.priority);
    proto.set_retry_times(core.retry_times);
    proto.set_status(core.status);
}

void convertFromProto(TaskInfoCore & core, const Protos::AutoStats::TaskInfoCore & proto)
{
    core.task_uuid = RPCHelpers::createUUID(proto.task_uuid());
    core.task_type = proto.task_type();
    core.table = StatsTableIdentifier(RPCHelpers::createStorageID(proto.storage_id()));
    for (const auto & column_name : proto.columns_name())
        core.columns_name.push_back(column_name);
    core.settings_json = proto.settings_json();
    core.stats_row_count = proto.stats_row_count();
    core.udi_count = proto.udi_count();
    core.priority = proto.priority();
    core.retry_times = proto.retry_times();
    core.status = proto.status();
}

// auto stats
void queryUdiCounter(ContextPtr context, const Protos::QueryUdiCounterReq * request, Protos::QueryUdiCounterResp * response)
{
    (void)request;
    (void)context;
    // TODO: move memory record into context
    auto & instance = AutoStatisticsMemoryRecord::instance();
    auto result = instance.getAndClearAll();

    for (auto & [uuid, udi_count] : result)
    {
        RPCHelpers::fillUUID(uuid, *response->add_tables());
        response->add_udi_counts(udi_count);
    }
}

// write to internal_memory_record of AutoStatsManager
void redirectUdiCounter(ContextPtr context, const Protos::RedirectUdiCounterReq * request, Protos::RedirectUdiCounterResp * response)
{
    (void)response;

    auto * manager = context->getAutoStatisticsManager();
    if (!manager)
        return;

    std::unordered_map<UUID, UInt64> result;
    assert(request->tables_size() == request->udi_counts_size());
    for (int i = 0; i < request->tables_size(); ++i)
    {
        auto uuid = RPCHelpers::createUUID(request->tables(i));
        auto count = request->udi_counts(i);
        result[uuid] = count;
    }
    manager->writeMemoryRecord(result);
}

void redirectAsyncStatsTasks(ContextPtr context, const Protos::RedirectAsyncStatsTasksReq * request, Protos::RedirectAsyncStatsTasksResp *)
{
    for (auto & task : request->tasks())
    {
        TaskInfoCore core;
        convertFromProto(core, task);
        AutoStats::writeTaskLog(context, core);
    }
}

void submitAsyncTasks(ContextPtr context, const std::vector<CollectTarget> & collect_targets)
{
    std::vector<TaskInfoCore> local_tasks;
    using ProtoTasks = google::protobuf::RepeatedPtrField<Protos::AutoStats::TaskInfoCore>;
    std::unordered_map<HostWithPorts, ProtoTasks> remote_requests;
    auto catalog = createCatalogAdaptor(context);

    for (const auto & target : collect_targets)
    {
        auto storage = catalog->getStorageByTableId(target.table_identifier);
        if (!storage)
        {
            auto err_msg = fmt::format("Can't find table {} in catalog", target.table_identifier.getNameForLogs());
            throw Exception(err_msg, ErrorCodes::TABLE_IS_DROPPED);
        }

        TaskInfoCore core;
        core.status = Status::Created;
        core.priority = 100;
        core.retry_times = 0;
        core.stats_row_count = 0;
        core.udi_count = 0;
        core.task_type = TaskType::Manual;
        core.task_uuid = UUIDHelpers::generateV4();
        core.table = target.table_identifier;
        core.settings_json = target.settings.toJsonStr();
        if (target.implicit_all_columns)
            core.columns_name = {};
        else
        {
            core.columns_name.clear();
            for (const auto & col : target.columns_desc)
            {
                core.columns_name.emplace_back(col.name);
            }
        }

        local_tasks.emplace_back(std::move(core));
    }

    for (auto & task : local_tasks)
    {
        writeTaskLog(context, task);
    }
}

}
