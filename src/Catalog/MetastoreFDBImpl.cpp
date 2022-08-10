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

void MetastoreFDBImpl::MultiWrite::addPut(const std::string & key, const std::string & value, const String & expected_value, bool if_not_exists)
{
    FDB::PutRequest put;
    cached_values.emplace_back(std::make_shared<String>(key));
    put.key = StringRef(*(cached_values.back()));
    cached_values.emplace_back(std::make_shared<String>(value));
    put.value = StringRef(*(cached_values.back()));
    put.if_not_exists = if_not_exists;
    if (!expected_value.empty())
    {
         cached_values.emplace_back(std::make_shared<String>(expected_value));
         put.expected_value = StringRef(*(cached_values.back()));
    }
    
    w_req.AddPut(put);
}

void MetastoreFDBImpl::MultiWrite::addDelete(const String & key, const UInt64 &)
{
    w_req.AddDelete(key);
}

bool MetastoreFDBImpl::MultiWrite::commit(bool allow_cas_fail)
{
    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    fdb_error_t error_code = fdb_client->MultiWrite(tr, w_req, w_resp);
    if (error_code == FDB::FDBError::FDB_not_committed && allow_cas_fail)
        return false;
    else
        check_fdb_op(error_code);
    return true;
}

MetastoreFDBImpl::MetastoreFDBImpl(const String & cluster_config_path)
{
    fdb_client = std::make_shared<FDB::FDBClient>(cluster_config_path);
}

MultiWritePtr MetastoreFDBImpl::createMultiWrite(bool )
{
    return std::make_shared<MultiWrite>(fdb_client);
}

void MetastoreFDBImpl::put(const String & key, const String & value, bool if_not_exists)
{
    FDB::PutRequest put_req;
    put_req.key = StringRef(key);
    put_req.value = StringRef(value);
    put_req.if_not_exists = if_not_exists;

    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    check_fdb_op(fdb_client->Put(tr, put_req));
}

bool MetastoreFDBImpl::putCAS(const String & key, const String & value, const String & expected)
{
    FDB::PutRequest put_req;
    put_req.key = StringRef(key);
    put_req.value = StringRef(value);
    put_req.expected_value = StringRef(expected);

    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    fdb_error_t code = fdb_client->Put(tr, put_req);
    if (!code)
        return true;
    else if (code != FDB::FDBError::FDB_not_committed)
        check_fdb_op(code);

    /// fail to put value due to transaction conflict.
    return false;
}

std::pair<bool, String> MetastoreFDBImpl::putCASWithOldValue(const String & key, const String & value, const String & expected)
{
    FDB::PutRequest put_req;
    put_req.key = StringRef(key);
    put_req.value = StringRef(value);
    put_req.expected_value = StringRef(expected);

    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    fdb_error_t code = fdb_client->Put(tr, put_req);

    if (code == FDB::FDBError::FDB_not_committed)
    {
        String latest_value;
        get(key, latest_value);
        return std::make_pair(false, std::move(latest_value));
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
    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    check_fdb_op(fdb_client->MultiGet(tr, keys, res));
    return res;
}

void MetastoreFDBImpl::drop(const String & key, const UInt64 &)
{
    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    check_fdb_op(fdb_client->Clear(tr, key, getNextKey(key)));
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

MetastoreFDBImpl::IteratorPtr MetastoreFDBImpl::getByPrefix(const String & prefix, const size_t &, uint32_t)
{
    FDB::ScanRequest scan_req;
    scan_req.start_key = prefix;
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

void MetastoreFDBImpl::multiPutCAS(const Strings & keys, const String & value_to_insert, const Strings & old_values, bool if_not_exists, std::vector<std::pair<uint32_t , String>> & cas_failed)
{
    ///sanity check
    if (!if_not_exists && keys.size() != old_values.size())
        throw Exception("Wrong arguments.", DB::ErrorCodes::METASTORE_OPERATION_ERROR);

    FDB::MultiWriteRequest req;
    FDB::MultiWriteResponse resp;

    for (size_t i=0; i<keys.size(); i++)
    {
        FDB::PutRequest single_put;
        single_put.key = StringRef(keys[i]);
        single_put.value = StringRef(value_to_insert);
        if (if_not_exists)
            single_put.if_not_exists = true;
        else
            single_put.expected_value = StringRef(old_values[i]);
        
        req.AddPut(single_put);
    }

    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    fdb_error_t error_code = fdb_client->MultiWrite(tr, req, resp);

    if (error_code == FDB::FDBError::FDB_not_committed && !resp.puts_.empty())
    {
        for (auto & put_resp : resp.puts_)
            cas_failed.emplace_back(put_resp);
    }
    else if (error_code == FDB::FDBError::FDB_not_committed)
    {
        auto latest_values = multiGet(keys);
        if (latest_values.size() != keys.size())
            throw Exception("Result size do not match request size. Logic error!", ErrorCodes::METASTORE_EXCEPTION);
        for (size_t i=0; i<latest_values.size(); i++)
        {
            if ((if_not_exists && latest_values[i].second)
                || (old_values.size() && latest_values[i].first != old_values[i]))
                cas_failed.emplace_back(i, latest_values[i].first);
        }
    }
    else
    {
        check_fdb_op(error_code);
    }
}

bool MetastoreFDBImpl::multiWriteCAS(const WriteRequests & requests)
{
    if (requests.empty())
        return true;

    FDB::MultiWriteRequest req;
    FDB::MultiWriteResponse resp;

    for (auto & write_req : requests)
    {
        FDB::PutRequest single_put;
        single_put.key = StringRef(write_req.key.data(), write_req.key.size());
        single_put.value = StringRef(write_req.new_value.data(), write_req.new_value.size());

        if (write_req.if_not_exists)
            single_put.if_not_exists = true;
        else if (write_req.old_value)
            single_put.expected_value = StringRef(write_req.old_value->data(), write_req.old_value->size());
        
        req.AddPut(single_put);
    }

    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    fdb_error_t error_code = fdb_client->MultiWrite(tr, req, resp);

    if (error_code == FDB::FDBError::FDB_not_committed && !resp.puts_.empty())
    {
        for (auto & put_resp : resp.puts_)
        {
            int index = std::get<0>(put_resp);
            if (requests[index].callback)
                requests[index].callback(error_code, String(fdb_get_error(error_code)));
        }
        return false;
    }
    else
    {
        check_fdb_op(error_code);
    }

    return true;
}

void MetastoreFDBImpl::clean(const String & prefix)
{
    String end_key = getNextKey(prefix);
    FDB::FDBTransactionPtr tr = std::make_shared<FDB::FDBTransactionRAII>();
    check_fdb_op(fdb_client->CreateTransaction(tr));
    check_fdb_op(fdb_client->Clear(tr, prefix, end_key));
}

void MetastoreFDBImpl::update(const String & key, const String & value)
{
    put(key, value);
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
