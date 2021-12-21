#pragma once

#include <Interpreters/HaReplicaHandler.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionaryManager.h>

#include <IO/ConnectionTimeouts.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <common/logger_useful.h>

namespace DB
{
class StorageHaMergeTree;
class BitEngineDictionaryHaManager;

class BitEngineDataService : public InterserverIOEndpoint
{
public:
    explicit BitEngineDataService(BitEngineDictionaryHaManager & bitengine_ha_manager);
    explicit BitEngineDataService(const BitEngineDataService &) = delete;
    BitEngineDataService & operator=(const BitEngineDataService &) = delete;

    String getId(const String & node_id) const override;

    void processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response) override;
    void onSendData(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response);
    void onSendIncrementData(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response);

private:
    BitEngineDictionaryHaManager & bitengine_ha_manager;
    Poco::Logger * log;
};

class BitEngineDataExchanger
{
public:
    explicit BitEngineDataExchanger(BitEngineDictionaryHaManager & bitengine_ha_manager);

    inline static String getEndpointId(const String & node_id) { return "BitEngineData:" + node_id; }

    void fetchData(const String & replica);
    void fetchIncrementData(const String & replica);

private:
    BitEngineDictionaryHaManager & bitengine_ha_manager;
    StorageHaMergeTree & storage;
};

using BitEngineDataExchangerPtr = std::shared_ptr<BitEngineDataExchanger>;

}
