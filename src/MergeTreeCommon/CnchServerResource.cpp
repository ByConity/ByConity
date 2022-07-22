#include <MergeTreeCommon/CnchServerResource.h>

#include <Catalog/DataModelPartWrapper.h>
#include <CloudServices/CnchWorkerResource.h>
#include <CloudServices/CnchWorkerClient.h>
#include <Interpreters/Context.h>

namespace DB
{

AssignedResource::AssignedResource(const StoragePtr & storage_):
    storage(storage_)
{}

void AssignedResource::addDataParts(const ServerDataPartsVector & parts)
{
    for (const auto & part: parts)
    {
        if (!part_names.count(part->name()))
        {
            part_names.emplace(part->name());
            server_parts.emplace_back(part);
        }
    }
}

void CnchServerResource::addCreateQuery(const ContextPtr & context, const StoragePtr & storage, const String & create_query)
{
    /// table should exists in SelectStreamFactory::createForShard
    /// so we create table in worker in advance
    if (context->getServerType() == ServerType::cnch_worker)
    {
        auto temp_context = Context::createCopy(context);
        auto worker_resource = context->getCnchWorkerResource();
        worker_resource->executeCreateQuery(temp_context, create_query, /* skip_if_exists */true);
    }

    auto lock = getLock();

    auto it = assigned_table_source.find(storage->getStorageUUID());
    if (it == assigned_table_source.end())
        it = assigned_table_source.emplace(storage->getStorageUUID(), AssignedResource{storage}).first;

    it->second.create_table_query = create_query;
}


void CnchServerResource::addBufferWorkers(const UUID & storage_id, const String & worker_table_name, const HostWithPortsVec & buffer_workers)
{
    auto lock = getLock();

    /// StorageID should exists.
    auto & assigned_resource = assigned_table_source.at(storage_id);

    assigned_resource.worker_table_name = worker_table_name;
    assigned_resource.buffer_workers = buffer_workers;
}

void CnchServerResource::sendResource(const ContextPtr & context, const HostWithPorts & worker)
{
    /// Skip local shard
    if (context->getServerType() == ServerType::cnch_worker && worker.getRPCAddress() == context->getHostWithPorts().getRPCAddress())
        return;

    allocateResource(context);

    std::vector<AssignedResource> resource_to_send;

    /// FIXME: mocked logic for sending create table query to all workersf
    {
        auto lock = getLock();
        std::vector<AssignedResource> resource_to_allocate;
        for (auto & [table_id, resource]: assigned_table_source)
        {
            if (resource.sent_create_query)
                continue;
            resource_to_allocate.emplace_back(resource);
            // assigned_resource.hive_parts.clear();
            resource.server_parts.clear();
            resource.sent_create_query = true;
        }

        const auto & worker_clients = context->getCurrentWorkerGroup()->getWorkerClients();
        std::vector<String> create_table_queries;
        for (auto & resource: resource_to_allocate)
        {
            LOG_DEBUG(log, "Prepare send create query: {} to {}", resource.create_table_query, worker_group->getQualifiedName());
            create_table_queries.emplace_back(resource.create_table_query);
        }

        for (const auto & worker_client: worker_clients)
        {
            /// FIXME: use isSameEndpoint()
            bool is_local = context->getServerType() == ServerType::cnch_worker
                && worker_client->getHostWithPorts().getRPCAddress() == context->getHostWithPorts().getRPCAddress();

            /// we already create table in local shard, skip it.
            if (is_local)
                continue;

            worker_client->sendCreateQueries(context, create_table_queries);
        }
    }

    /// FIXME: add below part after allocation logic is finished
    // {
    //     auto lock = getLock();
    //     auto it = assigned_worker_resource.find(worker);
    //     if (it == assigned_worker_resource.end())
    //         return;

    //     resource_to_send = std::move(it->second);
    //     assigned_worker_resource.erase(it);
    // }

    // auto worker_client = worker_group->getWorkerClient(worker);

    // {
    //     /// send create queries.
    //     std::vector<String> create_queries;

    //     for (auto & resource: resource_to_send)
    //     {
    //         if (!resource.sent_create_query)
    //             create_queries.emplace_back(resource.create_table_query);
    //     }
    //     worker_client->sendCreateQueries(context, create_queries);
    // }


    // for (auto & resource: resource_to_send)
    // {
    //     worker_client->sendQueryDataParts(
    //         context, resource.storage, resource.worker_table_name, resource.server_parts, resource.bucket_numbers);
    // }

    /// TODO: send offloading info.
}

void CnchServerResource::allocateResource(const ContextPtr & /*context*/)
{
    std::vector<AssignedResource> resource_to_allocate;
    {
        auto lock = getLock();
        for (auto & [table_id, resource]: assigned_table_source)
        {
            if (resource.server_parts.empty() && resource.sent_create_query)
                continue;
            resource_to_allocate.emplace_back(resource);
            // assigned_resource.hive_parts.clear();
            resource.server_parts.clear();
            resource.sent_create_query = true;
        }
    }

    if (resource_to_allocate.empty())
        return;

    [[maybe_unused]] const auto & host_ports_vec = worker_group->getHostWithPortsVec();
    [[maybe_unused]] const auto & worker_clients = worker_group->getWorkerClients();
    /// TODO: assigned data_parts.

    for ([[maybe_unused]]auto & resource: resource_to_allocate)
    {
        // const auto & storage = resource.storage;
        // auto & server_parts = resource.server_parts;
        // const auto & bucket_numbers = resource.bucket_numbers;
        // const auto & worker_table_name = resource.worker_table_name;

        // ServerAssignmentMap assigned_map;
        // BucketNumberAndServerPartsAssignment assigned_bucket_parts;

        // bool is_bucket_table = isCnchBucketTable(context, *storage, server_parts);
        // if (is_bucket_table)
        // {
        //     assigned_bucket_parts = assignCnchPartsForBucketTable(server_parts, worker_clients.size(), bucket_numbers);
        // }
        // else
        // {
        //     // part sorting is only needed by normal table for bounded consistent hash.
        //     sort_server_parts(context, server_parts);
        //     assigned_map = assignCnchParts(worker_group, server_parts);
        // }

        // for (size_t i = 0; i < host_ports_vec.size(); ++i)
        // {
        //     Stopwatch schedule_watch;

        //     ServerDataPartsVector assigned_parts;

        //     if (is_bucket_table)
        //     {
        //         assigned_parts = std::move(assigned_bucket_parts.parts_assignment_vec[i]);
        //     }
        //     else if (context.getSettingsRef().enable_virtual_part)
        //     {
        //         assigned_parts = server_parts;
        //     }
        //     else if (auto it = assigned_map.find(worker_ids[i]); it != assigned_map.end())
        //     {
        //         assigned_parts = std::move(it->second);
        //         /// Expand partial chain
        //     }

        //     auto it = assigned_worker_resource.find(host_ports_vec[i]);
        //     if (it == assigned_worker_resource.end())
        //     {
        //         it = assigned_worker_resource.emplace(host_ports_vec[i], std::vector<AssignedResource>{}).first;
        //     }

        //     it->second.emplace(AssignedResource{storage, std::move(assigned_parts), resource.sent_create_query});
        // }
    }
}

}
