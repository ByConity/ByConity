#pragma once
#include <Interpreters/HaReplicaHandler.h>

#include <common/logger_useful.h>

namespace DB
{
class IStorage;
using StorageWeakPtr = std::weak_ptr<IStorage>;

class StorageHaMergeTree;
class StorageHaUniqueMergeTree;

class ReadBuffer;
class WriteBuffer;

/**
 *  Reply message for HaMergeTree.
 *  Begin to work after queue is initialized.
 */

class HaMergeTreeReplicaEndpoint : public HaDefaultReplicaEndpoint
{
public:
    explicit HaMergeTreeReplicaEndpoint(StorageHaMergeTree & storage_);

private:
    void processPacket(UInt64, ReadBuffer & in, WriteBuffer & out) override;

    void onPutLogEntries(ReadBuffer & in, WriteBuffer & out);
    void onFetchLogEntries(ReadBuffer & in, WriteBuffer & out);
    void onGetDelay(ReadBuffer & in, WriteBuffer & out);
    void onCheckPartsExist(ReadBuffer & in, WriteBuffer & out);
    void onFindActiveContainingPart(ReadBuffer & in, WriteBuffer & out);
    void onGetLSNStatus(ReadBuffer & in, WriteBuffer & out);
    void onGetMutationStatus(ReadBuffer & in, WriteBuffer & out);

private:
    StorageHaMergeTree & storage;
    StorageWeakPtr weak_storage;
    Poco::Logger * log {nullptr};
};

using HaMergeTreeReplicaEndpointPtr = std::shared_ptr<HaMergeTreeReplicaEndpoint>;

/*
class HaUniqueMergeTreeReplicaEndpoint : public HaDefaultReplicaEndpoint
{
public:
    explicit HaUniqueMergeTreeReplicaEndpoint(StorageHaUniqueMergeTree & storage);

private:
    void processPacket(UInt64 packet_type, ReadBuffer & in, WriteBuffer & out) override;

    void onFetchManifestLogs(ReadBuffer & in, WriteBuffer & out);
    void onGetManifestStatus(ReadBuffer & in, WriteBuffer & out);
    void onGetManifestSnapshot(ReadBuffer & in, WriteBuffer & out);

    StorageHaUniqueMergeTree & storage;
    StorageWeakPtr weak_storage;
    Poco::Logger * logger;
};

using HaUniqueMergeTreeReplicaEndpointPtr = std::shared_ptr<HaUniqueMergeTreeReplicaEndpoint>;
*/

} // end of namespace DB
