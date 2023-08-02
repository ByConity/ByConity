#include "Interpreters/broadcastUDFQueryToCluster.h"
#include <Parsers/queryToString.h>
#include "DataStreams/RemoteBlockInputStream.h"

namespace DB
{

void broadcastUDFQueryToCluster(const ASTPtr & query_ptr, ContextMutablePtr context) {
    auto cluster = context->mockCnchServersCluster();
    if (!cluster) {
        return;
    }
    auto query = queryToString(*query_ptr);
    ThreadPool udf_pool(std::min<UInt64>(context->getSettingsRef().udf_ddl_pool_size, cluster->getShardCount()));
    for (const auto & shard_info : cluster->getShardsInfo()) {
        auto task = [&shard_info, &query, &context] {
            if (!shard_info.isLocal()) {
                Block emptyHeader{};
                DB::RemoteBlockInputStream remote_stream(shard_info.pool, query, emptyHeader, context);
                remote_stream.setPoolMode(PoolMode::GET_ONE);
                remote_stream.readPrefix();
                while (Block block = remote_stream.read());
                remote_stream.readSuffix();
            }
        };
        udf_pool.trySchedule(task);
    }
    udf_pool.wait();
}

// isHostServer is used to identify if this server is the one which orginally got the create/delete query.
// is_internal is when the query is fired internally.
// client_type was used to know where did this request come from. In case client_type = server, that means
// server has broadcasted this query. In case client_type = UNKNOWN means that it has come from actual client source.
bool isHostServer(bool is_internal, const ContextMutablePtr context) {
    return !is_internal && context->getClientInfo().client_type == ClientInfo::ClientType::UNKNOWN;
}

}
