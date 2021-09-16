#include <Storages/MergeTree/HaMergeTreeReplicaClient.h>

#include <Client/TimeoutSetter.h>
#include <Common/Exception.h>
#include <Core/Protocol.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

void HaMergeTreeReplicaClient::putLogEntry(HaMergeTreeLogEntryPtr entry)
{
    putLogEntries({std::move(entry)});
}

void HaMergeTreeReplicaClient::putLogEntries(const std::vector<HaMergeTreeLogEntryPtr> & entries)
{
    writeVarUInt(Protocol::HaClient::Put, *out);
    writeVarUInt(entries.size(), *out);
    for (auto & e : entries)
    {
        e->writeText(*out);
    }
    out->next();

    receiveOK();
}

std::vector<HaMergeTreeLogEntryPtr> HaMergeTreeReplicaClient::fetchLogEntries(const std::vector<UInt64> & lsns)
{
    writeVarUInt(Protocol::HaClient::Fetch, *out);
    writeVectorBinary(lsns, *out);
    out->next();

    receivePacketTypeOrThrow(Protocol::HaServer::LogEntry);

    size_t size{0};
    readVarUInt(size, *in);
    std::vector<HaMergeTreeLogEntryPtr> res(size);
    for (size_t i = 0; i < size; ++i)
    {
        res[i] = std::make_shared<HaMergeTreeLogEntry>();
        res[i]->readText(*in);
        /// We always mark the entries from remote executed
        res[i]->is_executed = false;
    }
    return res;
}

Int64 HaMergeTreeReplicaClient::getDelay()
{
    TimeoutSetter timeout_setter(*socket, sync_request_timeout, true);

    writeVarUInt(Protocol::HaClient::GetDelay, *out);
    out->next();

    receivePacketTypeOrThrow(Protocol::HaServer::Data);

    Int64 delay {0};
    readVarUInt(delay, *in);
    return delay;
}

bool HaMergeTreeReplicaClient::checkPartExist(const String & name, UInt64 & remote_num_send)
{
    TimeoutSetter timeout_setter(*socket, sync_request_timeout, true);

    writeVarUInt(Protocol::HaClient::CheckPartsExist, *out);
    writeVarUInt(1, *out);
    writeStringBinary(name, *out);
    out->next();

    receivePacketTypeOrThrow(Protocol::HaServer::Data);

    readVarUInt(remote_num_send, *in);

    UInt64 exist = false;
    readVarUInt(exist, *in);
    return exist;
}

Strings HaMergeTreeReplicaClient::findActiveContainingPart(const Strings & names, UInt64 & remote_num_send)
{
    TimeoutSetter timeout_setter(*socket, sync_request_timeout, true);

    writeVarUInt(Protocol::HaClient::FindActiveContainingPart, *out);
    writeVarUInt(names.size(), *out);
    for (auto & name : names)
        writeStringBinary(name, *out);
    out->next();

    receivePacketTypeOrThrow(Protocol::HaServer::Data);

    readVarUInt(remote_num_send, *in);

    Strings res;
    res.reserve(names.size());
    for (size_t i = 0; i < names.size(); ++i)
    {
        String containing_part;
        readStringBinary(containing_part, *in);
        res.push_back(containing_part);
    }

    return res;
}

LSNStatus HaMergeTreeReplicaClient::getLSNStatus()
{
    writeVarUInt(Protocol::HaClient::GetLSNStatus, *out);
    out->next();

    receivePacketTypeOrThrow(Protocol::HaServer::Data);

    LSNStatus lsn_status;
    readVarUInt(lsn_status.committed_lsn, *in);
    readVarUInt(lsn_status.updated_lsn, *in);
    readVarUInt(lsn_status.max_lsn, *in);
    return lsn_status;
}

/// TODO
#if 0
ManifestStore::LogEntries HaMergeTreeReplicaClient::fetchManifestLogs(UInt64 from, UInt64 limit)
{
    writeVarUInt(Protocol::HaClient::FetchManifestLogs, *out);
    writeVarUInt(from, *out);
    writeVarUInt(limit, *out);
    out->next();

    receivePacketTypeOrThrow(Protocol::HaServer::LogEntry);
    UInt64 size {0};
    readVarUInt(size, *in);

    ManifestStore::LogEntries res;
    res.resize(size);
    for (size_t i = 0; i < size; ++i)
        res[i].readText(*in);
    return res;
}

ManifestStatus HaMergeTreeReplicaClient::getManifestStatus()
{
    writeVarUInt(Protocol::HaClient::GetManifestStatus, *out);
    out->next();

    receivePacketTypeOrThrow(Protocol::HaServer::Data);
    ManifestStatus res {};
    readIntBinary(res.is_leader, *in);
    readVarUInt(res.latest_version, *in);
    readVarUInt(res.commit_version, *in);
    readVarUInt(res.checkpoint_version, *in);
    readVarUInt(res.num_running_sends, *in);
    return res;
}

ManifestStore::Snapshot HaMergeTreeReplicaClient::getManifestSnapshot(UInt64 version)
{
    writeVarUInt(Protocol::HaClient::GetManifestSnapshot, *out);
    writeVarUInt(version, *out);
    out->next();

    receivePacketTypeOrThrow(Protocol::HaServer::Data);
    ManifestStore::Snapshot res;
    UInt64 size {0};
    readVarUInt(size, *in);
    for (size_t i = 0; i < size; ++i)
    {
        String part;
        UInt64 delete_version;
        readStringBinary(part, *in, /*MAX_STRING_SIZE=*/1 << 16);
        readVarUInt(delete_version, *in);
        res.parts.emplace(part, delete_version);
    }
    readStringBinary(res.metadata_str, *in);
    readStringBinary(res.columns_str, *in);
    return res;
}

#endif

GetMutationStatusResponse HaMergeTreeReplicaClient::getMutationStatus(const String & mutation_id)
{
    TimeoutSetter timeout_setter(*socket, sync_request_timeout, true);

    writeVarUInt(Protocol::HaClient::GetMutationStatus, *out);
    writeStringBinary(mutation_id, *out);
    out->next();

    receivePacketTypeOrThrow(Protocol::HaServer::Data);
    GetMutationStatusResponse res;
    readStringBinary(res.mutation_pointer, *in, /*MAX_STRING_SIZE=*/1 << 6);
    readStringBinary(res.mutation_id, *in, /*MAX_STRING_SIZE=*/1 << 6);
    if (mutation_id != res.mutation_id)
        throw Exception("Request status for mutation " + mutation_id + " but got " + res.mutation_id,
                        ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
    readBoolText(res.is_found, *in);
    readBoolText(res.is_done, *in);
    Int64 time_field;
    readIntBinary(time_field, *in);
    res.finish_time = static_cast<time_t>(time_field);
    return res;
}

} // end of namespace DB
