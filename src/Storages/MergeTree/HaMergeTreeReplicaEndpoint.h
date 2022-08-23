#pragma once
#include <Interpreters/HaReplicaHandler.h>

#include <common/logger_useful.h>

namespace DB
{
class IStorage;
using StorageWeakPtr = std::weak_ptr<IStorage>;

class StorageHaMergeTree;

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
    void onDependedNumLog(ReadBuffer & in, WriteBuffer & out);
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

} // end of namespace DB
