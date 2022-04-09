#pragma once

#include <Client/HaConnection.h>
#include <Storages/MergeTree/HaConnectionMessages.h>
#include <Storages/MergeTree/HaMergeTreeLogEntry.h>
#include <Storages/MergeTree/LSNStatus.h>
#include <Storages/MergeTree/ManifestStore.h>

namespace DB
{

class HaMergeTreeReplicaClient : public HaConnection
{
public:
    using HaConnection::HaConnection;

    String getName() override { return "HaMergeTreeReplicaClient"; }

    void putLogEntry(HaMergeTreeLogEntryPtr entry);
    void putLogEntries(const std::vector<HaMergeTreeLogEntryPtr> & entries);

    std::vector<HaMergeTreeLogEntryPtr> fetchLogEntries(const std::vector<UInt64> & lsns);

    Int64 getDelay();
    Int64 getDependedNumLog(String target_replica);
    bool checkPartExist(const String & name, UInt64 & remote_num_send);
    Strings findActiveContainingPart(const Strings & names, UInt64 & remote_num_send);
    LSNStatus getLSNStatus();

    ManifestStore::LogEntries fetchManifestLogs(UInt64 from, UInt64 limit = 0);
    ManifestStatus getManifestStatus();
    ManifestStore::Snapshot getManifestSnapshot(UInt64 version);

    GetMutationStatusResponse getMutationStatus(const String & mutation_id);
};

using HaMergeTreeReplicaClientPtr = std::shared_ptr<HaMergeTreeReplicaClient>;

} // end of namespace DB
