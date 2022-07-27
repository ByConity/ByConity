#include <CloudServices/CnchServerResource.h>

#include <Catalog/DataModelPartWrapper.h>
#include <CloudServices/CnchWorkerResource.h>
#include <CloudServices/CnchWorkerClient.h>
#include <Interpreters/Context.h>
#include <MergeTreeCommon/assignCnchParts.h>


namespace DB
{

AssignedResource::AssignedResource(const StoragePtr & storage_):
    storage(storage_)
{}

void AssignedResource::addDataParts(const ServerDataPartsVector & parts)
{
    for (const auto & part: parts)
    {
        if (!part_names.contains(part->name()))
        {
            part_names.emplace(part->name());
            server_parts.emplace_back(part);
        }
    }
}

CnchServerResource::~CnchServerResource()
{
    if (!worker_group)
        return;

    auto worker_clients = worker_group->getWorkerClients();

    for (auto & worker_client: worker_clients)
    {
        try
        {
            worker_client->removeWorkerResource(txn_id);
        }
        catch (...)
        {
            tryLogCurrentException(
                __PRETTY_FUNCTION__,
                "Error occurs when remove WorkerResource{" + txn_id.toString() + "} in worker " + worker_client->getRPCAddress());
        }
    }
}

void CnchServerResource::addCreateQuery(const ContextPtr & context, const StoragePtr & storage, const String & create_query, const String & worker_table_name)
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

    auto it = assigned_table_resource.find(storage->getStorageUUID());
    if (it == assigned_table_resource.end())
        it = assigned_table_resource.emplace(storage->getStorageUUID(), AssignedResource{storage}).first;

    it->second.create_table_query = create_query;
    it->second.worker_table_name = worker_table_name;
}


void CnchServerResource::addBufferWorkers(const UUID & storage_id, const HostWithPortsVec & buffer_workers)
{
    auto lock = getLock();

    /// StorageID should exists.
    auto & assigned_resource = assigned_table_resource.at(storage_id);
    assigned_resource.buffer_workers = buffer_workers;
}

void CnchServerResource::sendResource(const ContextPtr & context, const HostWithPorts & worker)
{
    std::vector<AssignedResource> resource_to_send;

    {
        auto lock = getLock();
        allocateResource(context, lock);

        auto it = assigned_worker_resource.find(worker);
        if (it == assigned_worker_resource.end())
            return;

        resource_to_send = std::move(it->second);
        assigned_worker_resource.erase(it);
    }

    auto worker_client = worker_group->getWorkerClient(worker);

    bool is_local = context->getServerType() == ServerType::cnch_worker && worker.getRPCAddress() == context->getHostWithPorts().getRPCAddress();

    if (!is_local)
    {
        /// skip send create query to local shard in offloading mode
        std::vector<String> create_queries;
        for (auto & resource: resource_to_send)
        {
            if (!resource.sent_create_query)
                create_queries.emplace_back(resource.create_table_query);
        }
        worker_client->sendCreateQueries(context, create_queries);
    }

    /// send data parts.
    for (auto & resource: resource_to_send)
    {
        worker_client->sendQueryDataParts(
            context, resource.storage, resource.worker_table_name, resource.server_parts, resource.bucket_numbers);
    }

    /// TODO: send offloading info.
}

void CnchServerResource::allocateResource(const ContextPtr & context, std::lock_guard<std::mutex> &)
{
    std::vector<AssignedResource> resource_to_allocate;

    for (auto & [table_id, resource]: assigned_table_resource)
    {
        if (resource.empty())
            continue;

        resource_to_allocate.emplace_back(resource);
        // resource.hive_parts.clear();
        resource.server_parts.clear();
        resource.sent_create_query = true;
    }

    if (resource_to_allocate.empty())
        return;

    if (!worker_group)
        worker_group = context->getCurrentWorkerGroup();

    const auto & host_ports_vec = worker_group->getHostWithPortsVec();

    /// TODO: assign data_parts for bucket tables.
    for (auto & resource: resource_to_allocate)
    {
        const auto & storage = resource.storage;
        const auto & server_parts = resource.server_parts;
        auto assigned_map = assignCnchParts(worker_group, server_parts);

        for (const auto & host_ports : host_ports_vec)
        {
            ServerDataPartsVector assigned_parts;

            if (auto it = assigned_map.find(host_ports.id); it != assigned_map.end())
            {
                assigned_parts = std::move(it->second);
            }

            auto it = assigned_worker_resource.find(host_ports);
            if (it == assigned_worker_resource.end())
            {
                it = assigned_worker_resource.emplace(host_ports, std::vector<AssignedResource>{}).first;
            }

            it->second.emplace_back(storage);
            auto & worker_resource = it->second.back();

            worker_resource.addDataParts(assigned_parts);
            worker_resource.sent_create_query = resource.sent_create_query;
            worker_resource.create_table_query = resource.create_table_query;
            worker_resource.worker_table_name = resource.worker_table_name;
        }
    }
}

}
