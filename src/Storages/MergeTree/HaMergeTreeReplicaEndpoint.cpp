#include <Storages/MergeTree/HaMergeTreeReplicaEndpoint.h>

#include <Core/Protocol.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/HaMergeTreeLogManager.h>
#include <Storages/StorageHaMergeTree.h>
/// TODO: #include <Storages/StorageHaUniqueMergeTree.h>

namespace CurrentMetrics
{
extern const Metric ReplicatedSend;
}

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TABLE;
}

HaMergeTreeReplicaEndpoint::HaMergeTreeReplicaEndpoint(StorageHaMergeTree & storage_)
    : storage(storage_)
    , weak_storage(storage.shared_from_this())
    , log(&Poco::Logger::get(storage.getStorageID().getFullTableName() + " (HaMergeTreeReplicaEndpoint)"))
{
}

void HaMergeTreeReplicaEndpoint::processPacket(UInt64 packet_type, ReadBuffer & in, WriteBuffer & out)
{
    auto owned_storage = weak_storage.lock();
    if (!owned_storage)
        throw Exception("The table was already dropped", ErrorCodes::UNKNOWN_TABLE);

    switch (packet_type)
    {
        case Protocol::HaClient::Put:
            onPutLogEntries(in, out);
            return;

        case Protocol::HaClient::Fetch:
            onFetchLogEntries(in, out);
            return;

        case Protocol::HaClient::GetDelay:
            onGetDelay(in, out);
            return;

        case Protocol::HaClient::CheckPartsExist:
            onCheckPartsExist(in, out);
            return;

        case Protocol::HaClient::FindActiveContainingPart:
            onFindActiveContainingPart(in, out);
            return;

        case Protocol::HaClient::GetLSNStatus:
            onGetLSNStatus(in, out);
            return;

        case Protocol::HaClient::GetMutationStatus:
            /// TODO:
            /// onGetMutationStatus(in, out);
            return;

        default:
            HaDefaultReplicaEndpoint::processPacket(packet_type, in, out);
    }
}

void HaMergeTreeReplicaEndpoint::onPutLogEntries(ReadBuffer & in, WriteBuffer & out)
{
    size_t size {0};
    readVarUInt(size, in);

    std::vector<HaMergeTreeLogEntryPtr> entries(size);
    for (size_t i = 0; i < size; ++i)
    {
        entries[i] = std::make_shared<HaMergeTreeLogEntry>();
        entries[i]->readText(in);
        /// We always mark the entries from remote not executed
        entries[i]->is_executed = false;
    }

    LOG_TRACE(log, "Received {} entries.", entries.size());
    storage.queue.write(std::move(entries), false); // broadcast = false

    writeVarUInt(Protocol::HaServer::OK, out);
    out.next();
}


void HaMergeTreeReplicaEndpoint::onFetchLogEntries(ReadBuffer & in, WriteBuffer & out)
{
    std::vector<UInt64> lsns;
    readVectorBinary(lsns, in);

    if (!std::is_sorted(lsns.begin(), lsns.end()))
        throw Exception("Expected sorted request LSN list.", ErrorCodes::LOGICAL_ERROR);

    LOG_TRACE(log, "Other replica requests {} LSNs.", lsns.size());

    HaMergeTreeLogEntry::Vec res;
    {
        auto lock = storage.log_manager->lockMe();
        res = logSetIntersection(storage.log_manager->getEntryListUnsafe(), lsns);
    }

    LOG_TRACE(log, "Response {} entries", res.size());

    writeVarUInt(Protocol::HaServer::LogEntry, out);
    writeVarUInt(res.size(), out);
    for (auto & e : res)
    {
        e->writeText(out);
    }
    out.next();
}

void HaMergeTreeReplicaEndpoint::onGetDelay(ReadBuffer & , WriteBuffer & out)
{
    Int64 absolute_delay = storage.getAbsoluteDelay();
    writeVarUInt(Protocol::HaServer::Data, out);
    writeVarUInt(absolute_delay, out);
    out.next();
}

void HaMergeTreeReplicaEndpoint::onCheckPartsExist(ReadBuffer & in, WriteBuffer & out)
{
    size_t num_parts{0};
    readVarUInt(num_parts, in);

    std::vector<String> names(num_parts);
    for (auto & name : names)
        readStringBinary(name, in);

    /// check whether parts exist
    std::vector<UInt64> results;
    results.reserve(num_parts);

    {
        auto storage_lock = storage.lockForShare(RWLockImpl::NO_QUERY, storage.getContext()->getSettingsRef().lock_acquire_timeout);
        for (auto & name : names)
        {
            auto part = storage.getPartIfExists(
                name,
                {
                    MergeTreeDataPartState::PreCommitted,
                    MergeTreeDataPartState::Committed,
                    MergeTreeDataPartState::Outdated,
                });
            results.push_back(part ? 1 : 0);
        }
    }

    /// send metric & results
    writeVarUInt(Protocol::HaServer::Data, out);
    writeVarUInt(CurrentMetrics::values[CurrentMetrics::ReplicatedSend].load(std::memory_order_relaxed), out);
    for (auto exist : results)
        writeVarUInt(exist, out);
    out.next();
}

void HaMergeTreeReplicaEndpoint::onFindActiveContainingPart(ReadBuffer & in, WriteBuffer & out)
{
    size_t num_parts{0};
    readVarUInt(num_parts, in);

    std::vector<String> names(num_parts);
    for (auto & name : names)
        readStringBinary(name, in);

    std::vector<String> res_names;
    /// calculate the result before writing the response
    /// so that any exception raised would be written as an exception packet
    {
        auto storage_lock = storage.lockForShare(RWLockImpl::NO_QUERY, storage.getContext()->getSettingsRef().lock_acquire_timeout);
        for (auto & name : names)
        {
            if (auto part = storage.getActiveContainingPart(name))
                res_names.push_back(part->name);
            else
                res_names.emplace_back();
        }
    }

    /// send metric & results
    writeVarUInt(Protocol::HaServer::Data, out);
    writeVarUInt(CurrentMetrics::values[CurrentMetrics::ReplicatedSend].load(std::memory_order_relaxed), out);
    for (auto & name : res_names)
        writeStringBinary(name, out);

    out.next();
}

void HaMergeTreeReplicaEndpoint::onGetLSNStatus(ReadBuffer &, WriteBuffer & out)
{
    writeVarUInt(Protocol::HaServer::Data, out);
    auto lsn_status = storage.log_manager->getLSNStatus(true);
    writeVarUInt(lsn_status.committed_lsn, out);
    writeVarUInt(lsn_status.updated_lsn, out);
    writeVarUInt(lsn_status.max_lsn, out);
    out.next();
}

#if 0
void HaMergeTreeReplicaEndpoint::onGetMutationStatus(ReadBuffer & in, WriteBuffer & out)
{
    String mutation_id;
    readStringBinary(mutation_id, in);

    String mutation_pointer;
    auto status = storage.queue.getPartialMutationsStatus(mutation_id, /*is_alter_metadata=*/false, &mutation_pointer);
    bool is_found = status.has_value();
    bool is_done = is_found && status->is_done;
    Int64 finish_time = is_found ? static_cast<Int64>(status->finish_time) : 0;

    writeVarUInt(Protocol::HaServer::Data, out);
    writeStringBinary(mutation_pointer, out);
    writeStringBinary(mutation_id, out);
    writeBoolText(is_found, out);
    writeBoolText(is_done, out);
    writeIntBinary(finish_time, out);
    out.next();
}
#endif

/*
HaUniqueMergeTreeReplicaEndpoint::HaUniqueMergeTreeReplicaEndpoint(StorageHaUniqueMergeTree & storage_)
    : storage(storage_)
    , weak_storage(storage.shared_from_this())
    , logger(&Poco::Logger::get("HaUniqueMergeTreeReplicaEndpoint (" + storage.getDatabaseName() + "." + storage.getTableName() + ")"))
{
}

void HaUniqueMergeTreeReplicaEndpoint::processPacket(UInt64 packet_type, ReadBuffer & in, WriteBuffer & out)
{
    auto owned_storage = weak_storage.lock();
    if (!owned_storage)
        throw Exception("The table was already dropped", ErrorCodes::UNKNOWN_TABLE);

    switch (packet_type)
    {
        case Protocol::HaClient::FetchManifestLogs:
            onFetchManifestLogs(in, out);
            return;

        case Protocol::HaClient::GetManifestStatus:
            onGetManifestStatus(in, out);
            return;

        case Protocol::HaClient::GetManifestSnapshot:
            onGetManifestSnapshot(in, out);
            return;

        default:
            HaDefaultReplicaEndpoint::processPacket(packet_type, in, out);
    }
}

void HaUniqueMergeTreeReplicaEndpoint::onFetchManifestLogs(ReadBuffer & in, WriteBuffer & out)
{
    UInt64 from, limit;
    readVarUInt(from, in);
    readVarUInt(limit, in);
    auto res = storage.manifest_store->getLogEntries(from, limit);
    writeVarUInt(Protocol::HaServer::LogEntry, out);
    writeVarUInt(res.size(), out);
    for (auto & e : res)
        e.writeText(out);
    out.next();
}

void HaUniqueMergeTreeReplicaEndpoint::onGetManifestStatus(ReadBuffer &, WriteBuffer & out)
{
    writeVarUInt(Protocol::HaServer::Data, out);
    writeIntBinary(UInt8(storage.is_leader), out);
    writeVarUInt(storage.manifest_store->latestVersion(), out);
    writeVarUInt(storage.manifest_store->commitVersion(), out);
    writeVarUInt(storage.manifest_store->checkpointVersion(), out);
    writeVarUInt(CurrentMetrics::values[CurrentMetrics::ReplicatedSend].load(std::memory_order_relaxed), out);
    out.next();
}

void HaUniqueMergeTreeReplicaEndpoint::onGetManifestSnapshot(ReadBuffer & in, WriteBuffer & out)
{
    UInt64 version;
    readVarUInt(version, in);
    auto commit_version = storage.manifest_store->commitVersion();
    if (version > commit_version)
        throw Exception("Request version " + toString(version) + " > commit version " + toString(commit_version),
                        ErrorCodes::BAD_ARGUMENTS);
    auto snapshot = storage.manifest_store->getSnapshot(version);
    writeVarUInt(Protocol::HaServer::Data, out);
    writeVarUInt(snapshot.parts.size(), out);
    for (auto & entry : snapshot.parts)
    {
        writeStringBinary(entry.first, out);
        writeVarUInt(entry.second, out);
    }
    writeStringBinary(snapshot.metadata_str, out);
    writeStringBinary(snapshot.columns_str, out);
    out.next();
}
*/

} // end of namespace DB
