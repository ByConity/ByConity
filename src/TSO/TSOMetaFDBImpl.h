/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <TSO/TSOMetastore.h>
#include <TSO/TSOOperations.h>
#include <Core/Types.h>
#include <Catalog/FDBClient.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TSO_OPERATION_ERROR;
};

namespace TSO
{

class TSOMetaFDBImpl : public TSOMetastore
{

public:
    TSOMetaFDBImpl(const String & cluster_file, const String & key_name_)
        : TSOMetastore(key_name_)
    {
        fdb_client = FDB::FDBClient::Instance(cluster_file);
    }

    ~TSOMetaFDBImpl() override {}

    void put(const String & value) override
    {
        FDB::PutRequest put_req;
        put_req.key = StringRef(key_name);
        put_req.value = StringRef(value);
        assertStatus(OperationType::PUT, fdb_client->Put(put_req));
    }

    void get(String & value) override
    {
        FDB::GetResponse res;
        assertStatus(OperationType::GET, fdb_client->Get(key_name, res));
        value = res.value;
    }

    void clean() override
    {
        assertStatus(OperationType::CLEAN, fdb_client->Delete(key_name));
    }

private:
    FDB::FDBClientPtr fdb_client;

    void assertStatus(const OperationType & op, const fdb_error_t & error_code)
    {
        if (error_code)
            throw Exception("Exception while executing operation : " + Operation(op) + ", Errormsg : " + String(fdb_get_error(error_code)) , ErrorCodes::TSO_OPERATION_ERROR);
    }
};

}

}
