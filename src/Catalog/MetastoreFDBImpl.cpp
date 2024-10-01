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

#include <Catalog/MetastoreFDBImpl.h>
#include <Catalog/CatalogUtils.h>
#include <Catalog/FDBError.h>
#include <common/defines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int METASTORE_OPERATION_ERROR;
    extern const int METASTORE_EXCEPTION;
    extern const int METASTORE_COMMIT_CAS_FAILURE;
    extern const int NOT_IMPLEMENTED;
}

namespace Catalog
{

MetastoreFDBImpl::MetastoreFDBImpl(const String & cluster_config_path) { fdb_client = FDB::FDBClient::Instance(cluster_config_path); }

void MetastoreFDBImpl::put(const String & key, const String & value, bool if_not_exists)
{
    assertNotReadonly(key);
    FDB::PutRequest put_req;
    put_req.key = StringRef(key);
    put_req.value = StringRef(value);
    put_req.if_not_exists = if_not_exists;

    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    check_fdb_op(fdb_client->Put(tr, put_req));
}

std::pair<bool, String> MetastoreFDBImpl::putCAS(const String & key, const String & value, const String & expected, bool with_old_value)
{
    assertNotReadonly(key);
    FDB::PutRequest put_req;
    put_req.key = StringRef(key);
    put_req.value = StringRef(value);
    put_req.expected_value = StringRef(expected);

    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    fdb_error_t code = fdb_client->Put(tr, put_req);

    if (code == FDB::FDBError::FDB_not_committed)
    {
        String old_value;
        if (with_old_value)
            get(key, old_value);

        return std::make_pair(false, std::move(old_value));
    }
    else
        check_fdb_op(code);

    return std::make_pair(true, "");
}

uint64_t MetastoreFDBImpl::get(const String & key, String & value)
{
    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    FDB::GetResponse res;
    check_fdb_op(fdb_client->Get(tr, key, res));
    if (res.is_present)
    {
        value = res.value;
        return 1;
    }
    else
        return 0;
}

std::vector<std::pair<String, UInt64>> MetastoreFDBImpl::multiGet(const std::vector<String> & keys)
{
    std::vector<std::pair<String, UInt64>> res;
    res.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); i += DEFAULT_MULTI_GET_BATCH_COUNT)
    {
        std::vector<String> batch_keys(keys.begin() + i, keys.begin() + std::min(i + DEFAULT_MULTI_GET_BATCH_COUNT, keys.size()));
        FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
        check_fdb_op(fdb_client->CreateTransaction(tr));
        check_fdb_op(fdb_client->MultiGet(tr, batch_keys, res));
    }
    return res;
}

void MetastoreFDBImpl::drop(const String & key, [[maybe_unused]] const UInt64 & expected)
{
    assertNotReadonly(key);
    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    check_fdb_op(fdb_client->Delete(tr, key));
}

void MetastoreFDBImpl::drop(const String & key, const String & expected_value)
{
    assertNotReadonly(key);
    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    check_fdb_op(fdb_client->Delete(tr, key, expected_value));
}

MetastoreFDBImpl::IteratorPtr MetastoreFDBImpl::getAll()
{
    FDB::ScanRequest scan_req;
    scan_req.start_key = String("");
    scan_req.end_key = String("\xff");

    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    auto fdb_iter = fdb_client->Scan(tr, scan_req);
    return std::make_shared<FDBIterator>(fdb_iter);
}

MetastoreFDBImpl::IteratorPtr MetastoreFDBImpl::getByPrefix(const String & prefix, const size_t & limit, uint32_t, const String & start_key)
{
    FDB::ScanRequest scan_req;

    if (likely(start_key.empty()))
    {
        scan_req.start_key = prefix;
    }
    else
    {
        scan_req.start_key = start_key;
    }
    scan_req.row_limit = limit;
    scan_req.end_key = getNextKey(prefix);

    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    auto fdb_iter = fdb_client->Scan(tr, scan_req);
    return std::make_shared<FDBIterator>(fdb_iter);
}

MetastoreFDBImpl::IteratorPtr MetastoreFDBImpl::getByRange(const String & range_start, const String & range_end, const bool include_start, const bool include_end)
{
    FDB::ScanRequest scan_req;
    scan_req.start_key = include_start ? range_start : getNextKey(range_start);
    scan_req.end_key = include_end ? getNextKey(range_end) : range_end;

    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    auto fdb_iter = fdb_client->Scan(tr, scan_req);
    return std::make_shared<FDBIterator>(fdb_iter);
}

bool MetastoreFDBImpl::batchWrite(const BatchCommitRequest & req, BatchCommitResponse & response)
{
    for (auto & single_put : req.puts)
    {
        assertNotReadonly(single_put.key);
    }

    for (auto & single_delete : req.deletes)
    {
        assertNotReadonly(single_delete.key);
    }

    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    fdb_error_t error_code = fdb_client->MultiWrite(tr, req, response);
    check_fdb_op(error_code);

    return true;
}

void MetastoreFDBImpl::clean(const String & prefix)
{
    String end_key = getNextKey(prefix);
    assertNotReadonly(end_key);
    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    check_fdb_op(fdb_client->Clear(tr, prefix, end_key));
}

void MetastoreFDBImpl::check_fdb_op(const fdb_error_t & error_code)
{
    if (error_code)
        throw Exception("FDB error : " + String(fdb_get_error(error_code)), toCommonErrorCode(error_code));
}

int MetastoreFDBImpl::toCommonErrorCode(const fdb_error_t & error_code)
{
    switch (error_code)
    {
    case FDB::FDBError::FDB_not_committed:
        return DB::ErrorCodes::METASTORE_COMMIT_CAS_FAILURE;

    default:
        return error_code;
    }
}

}

}
