
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDataExchanger.h>
#include <Storages/StorageHaMergeTree.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/InterserverCredentials.h>
#include <IO/ConnectionTimeoutsContext.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ACTIVE_REPLICAS;
}

class HTMLForm;
class HTTPServerResponse;

BitEngineDataService::BitEngineDataService(BitEngineDictionaryHaManager & bitengine_ha_manager_)
    : bitengine_ha_manager(bitengine_ha_manager_){}

String BitEngineDataService::getId(const String & node_id) const
{
    return "BitEngineData:" + node_id;
}

void BitEngineDataService::processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response)
{
    if (blocker.isCancelled())
        throw Exception("Transferring bitengine data to replica was cancelled", ErrorCodes::LOGICAL_ERROR);

    String qtype;
    try
    {
        qtype = params.get("qtype");
    }catch(Poco::NotFoundException &)
    {
        qtype = "FetchData";
    }catch(...)
    {
        throw;
    }

    if (qtype == "FetchData")
        onSendData(params, body, out, response);
    else if (qtype == "FetchIncrementData")
        onSendIncrementData(params, body, out, response);
    else
        throw Exception("Not support qtype: " + qtype, ErrorCodes::LOGICAL_ERROR);
}

void BitEngineDataService::onSendData(const HTMLForm & /*params*/, ReadBuffer & /*body*/, WriteBuffer & out, HTTPServerResponse & /*response*/)
{
    bitengine_ha_manager.writeData(out);
}

void BitEngineDataService::onSendIncrementData(const HTMLForm & params, ReadBuffer & /*body*/, WriteBuffer & out, HTTPServerResponse & /*response*/)
{
    String received_offsets;
    try
    {
        received_offsets = params.get("offsets");
    }
    catch(...)
    {
        throw;
    }
    
    //std::cout<<" onSendIncementData: "<< received_offsets << std::endl;
    ReadBufferFromString offsets_buf{received_offsets};
    IncrementOffset increment_offset = bitengine_ha_manager.readIncrementOffset(offsets_buf);
        
    bitengine_ha_manager.writeIncrementData(out, increment_offset);
}

BitEngineDataExchanger::BitEngineDataExchanger(BitEngineDictionaryHaManager & bitengine_ha_manager_)
    :bitengine_ha_manager(bitengine_ha_manager_), storage(bitengine_ha_manager_.storage) {}

void BitEngineDataExchanger::fetchData
    (
        const String & replica
    ){
    const auto replica_address = bitengine_ha_manager.getReplicaAddress(replica);
    const String host = replica_address.host;
    int port = replica_address.replication_port;

    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(storage.getContext()->getSettingsRef());
    auto credentials = storage.getContext()->getInterserverCredentials();
    const String interserver_scheme = storage.getContext()->getInterserverScheme();
    auto user = credentials->getUser();

    Poco::URI uri;
    uri.setScheme(interserver_scheme);
    uri.setScheme("http");
    uri.setHost(host);
    uri.setPort(port);
    uri.setQueryParameters(
        {
            {"qtype", "FetchData"},
            {"endpoint", getEndpointId(bitengine_ha_manager.getReplicaPath(replica))},
            {"compress", "false"}
        });

    Poco::Net::HTTPBasicCredentials creds{};
    if (!user.empty())
    {
        creds.setUsername(user);
        creds.setPassword(credentials->getPassword());
    }

    ReadWriteBufferFromHTTP in{uri, Poco::Net::HTTPRequest::HTTP_POST, {}, timeouts, 0, creds};

    bitengine_ha_manager.readData(in);
}

void BitEngineDataExchanger::fetchIncrementData
    (
        const String & replica
    ){
    auto replica_address = bitengine_ha_manager.getReplicaAddress(replica);
    const String host = replica_address.host;
    int port = replica_address.replication_port;

    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(storage.getContext()->getSettingsRef());
    auto credentials = storage.getContext()->getInterserverCredentials();
    const String interserver_scheme = storage.getContext()->getInterserverScheme();
    auto user = credentials->getUser();

    WriteBufferFromOwnString str_buf;
    bitengine_ha_manager.writeIncrementOffset(str_buf);
    String send_offsets = str_buf.str();
    //std::cout<<" onFetchIncementData: "<< send_offsets << std::endl;

    Poco::URI uri;
    uri.setScheme(interserver_scheme);
    uri.setScheme("http");
    uri.setHost(host);
    uri.setPort(port);
    uri.setQueryParameters(
        {
            {"qtype", "FetchIncrementData"},
            {"endpoint", getEndpointId(bitengine_ha_manager.getReplicaPath(replica))},
            {"offsets", send_offsets},
            {"compress", "false"}
        });

    Poco::Net::HTTPBasicCredentials creds{};
    if (!user.empty())
    {
        creds.setUsername(user);
        creds.setPassword(credentials->getPassword());
    }

    ReadWriteBufferFromHTTP in{uri, Poco::Net::HTTPRequest::HTTP_POST, {}, timeouts, 0, creds};

    bitengine_ha_manager.readIncrementData(in);
}

} // end of namespace DB

