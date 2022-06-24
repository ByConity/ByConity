#pragma once

#include <bytekv4cpp/bytekv/client.h>
#include <TSO/TSOOperations.h>
#include <Common/Exception.h>
#include <Core/Types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TSO_OPERATION_ERROR;
}

namespace TSO
{

using namespace bytekv::sdk;

class TSOByteKVImpl
{

public:
    using ExpectedCodes = std::initializer_list<Errorcode>;

    TSOByteKVImpl(
        const String & service_name_,
        const String & cluster_name_,
        const String & name_space_,
        const String & table_name_,
        const String & key_name_)
        : service_name(service_name_), cluster_name(cluster_name_), name_space(name_space_), table_name(table_name_), key_name(key_name_)
    {
        init();
    }

    void init()
    {
        auto code = ByteKVClientBuilder()
                .setServiceName(service_name)
                .setClusterName(cluster_name)
                .setNameSpace(name_space)
                .build(client);
        assertStatus(OperationType::OPEN, code, {Errorcode::OK});
    }

    void put(const String & value)
    {
        PutRequest put_req;
        PutResponse put_resp;
        put_req.table = this->table_name;
        put_req.key =  this->key_name;
        put_req.value = value;
        auto code = client->Put(put_req, &put_resp);
        assertStatus(OperationType::PUT, code, {Errorcode::OK});
    }

    void get(String & value)
    {
        GetRequest get_req;
        GetResponse get_resp;
        get_req.table = this->table_name;
        get_req.key = this->key_name;
        auto code = client->Get(get_req, &get_resp);
        assertStatus(OperationType::GET, code, {Errorcode::OK, Errorcode::KEY_NOT_FOUND});
        value = std::move(get_resp.value);
    }

    void clean()
    {
        DeleteRequest del_req;
        DeleteResponse del_resp;
        del_req.table = this->table_name;
        del_req.key = this->key_name;
        auto code = client->Delete(del_req, &del_resp);
        assertStatus(OperationType::CLEAN, code, {Errorcode::OK});
    }

public:
    std::shared_ptr<ByteKVClient> client;

private:
    String service_name;
    String cluster_name;
    String name_space;
    String table_name;
    String key_name;

private:
    void assertStatus(const OperationType & op, const Errorcode & code, const ExpectedCodes & expected)
    {
        for (auto expected_code : expected)
        {
            if (expected_code == code)
                return;
        }
        throw Exception("Unexpected result from byteKV. Operation : " + Operation(op) + ", Errormsg : " + ErrorString(code) , ErrorCodes::TSO_OPERATION_ERROR);
    }
};

}

}
