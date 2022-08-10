#pragma once
#include <TSO/TSOMetastore.h>
#include <TSO/TSOOperations.h>
#include <Core/Types.h>
#include <Catalog/FDBClient.h>

namespace DB
{

namespace TSO
{

class TSOMetaFDBImpl : public TSOMetastore
{

public:
    TSOMetaFDBImpl(const String & cluster_file, const String & key_name_)
        : TSOMetastore(key_name_)
    {
        fdb_client = std::make_shared<FDB::FDBClient>(cluster_file);
    }

    ~TSOMetaFDBImpl() override {}

    void put(const String & value) override
    {
        FDB::PutRequest put_req;
        put_req.key = StringRef(key_name);
        put_req.value = StringRef(value);
        FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
        assertStatus(OperationType::PUT, fdb_client->CreateTransaction(tr));
        assertStatus(OperationType::PUT, fdb_client->Put(tr, put_req));
    }

    void get(String & value) override
    {
        FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
        assertStatus(OperationType::GET, fdb_client->CreateTransaction(tr));
        FDB::GetResponse res;
        assertStatus(OperationType::GET, fdb_client->Get(tr, key_name, res));
        value = res.value;
    }

    void clean() override
    {
        FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
        assertStatus(OperationType::CLEAN, fdb_client->CreateTransaction(tr));
        assertStatus(OperationType::CLEAN, fdb_client->Delete(tr, key_name));
    }

private:
    FDB::FDBClientPtr fdb_client;

    void assertStatus(const OperationType & op, const fdb_error_t & error_code)
    {
        if (error_code)
            throw Exception("Exception whiline executing operation : " + Operation(op) + ", Errormsg : " + String(fdb_get_error(error_code)) , ErrorCodes::TSO_OPERATION_ERROR);
    }
};

}

}
