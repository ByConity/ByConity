#include <Catalog/MetastoreByteKVImpl.h>
#include <Catalog/CatalogUtils.h>
#include <iostream>
#include <common/defines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int METASTORE_OPERATION_ERROR;
    extern const int METASTORE_COMMIT_CAS_FAILURE;
}

namespace Catalog
{

using namespace bytekv::sdk;

MetastoreByteKVImpl::MetastoreByteKVImpl(const String & service_name_, const String & cluster_name_,
                                         const String & name_space_, const String & table_name_)
    : IMetaStore(), service_name(service_name_), cluster_name(cluster_name_), name_space(name_space_), table_name(table_name_)
{
    init();
}

void MetastoreByteKVImpl::init()
{
    auto code = ByteKVClientBuilder()
        .setServiceName(service_name)
        .setClusterName(cluster_name)
        .setNameSpace(name_space)
        .setConnectionTimeoutMS(3000)
        .setReadTimeoutMS(30000)
        .setWriteTimeoutMS(30000)
        .build(client);
    assertStatus(OperationType::OPEN, code, {Errorcode::OK});
}

void MetastoreByteKVImpl::put(const String & key, const String & value, bool if_not_exists)
{
    PutRequest put_req;
    PutResponse put_resp;
    put_req.table = this->table_name;
    put_req.key =  key;
    put_req.value = value;
    put_req.if_not_exists = if_not_exists;
    auto code = client->Put(put_req, &put_resp);
    assertStatus(OperationType::PUT, code, {Errorcode::OK});
}

void MetastoreByteKVImpl::putTTL(const String & key, const String & value, UInt64 ttl)
{
    PutRequest put_req{this->table_name, key, value, ttl};
    PutResponse put_resp;
    auto code = client->Put(put_req, &put_resp);
    assertStatus(OperationType::PUT, code, {Errorcode::OK});
}

bool MetastoreByteKVImpl::putCAS(const String & key, const String & value, const String & expected)
{
    PutRequest put_req;
    PutResponse put_resp;
    Slice expected_value{expected};
    put_req.table = this->table_name;
    put_req.key =  key;
    put_req.value = value;
    put_req.expected_value = &expected_value;
    auto code = client->Put(put_req, &put_resp);

    assertStatus(OperationType::PUT, code, {Errorcode::OK, Errorcode::CAS_FAILED});
    return code == Errorcode::OK;
}

std::pair<bool, String> MetastoreByteKVImpl::putCASWithOldValue(const String & key, const String & value, const String & expected)
{
    PutRequest put_req;
    PutResponse put_resp;
    Slice expected_value{expected};
    put_req.table = this->table_name;
    put_req.key = key;
    put_req.value = value;
    put_req.expected_value = &expected_value;
    auto code = client->Put(put_req, &put_resp);

    assertStatus(OperationType::PUT, code, {Errorcode::OK, Errorcode::CAS_FAILED});
    bool cas_res = (code == Errorcode::OK);
    String current_value = cas_res ? expected : put_resp.current_value;
    return std::make_pair(cas_res, std::move(current_value));
}

uint64_t MetastoreByteKVImpl::get(const String & key, String & value)
{
    GetRequest get_req;
    GetResponse get_resp;
    get_req.table = this->table_name;
    get_req.key = key;
    auto code = client->Get(get_req, &get_resp);
    assertStatus(OperationType::GET, code, {Errorcode::OK, Errorcode::KEY_NOT_FOUND});
    value = std::move(get_resp.value);
    return get_resp.version;
}

std::vector<std::pair<String, UInt64>> MetastoreByteKVImpl::multiGet(const std::vector<String> & keys)
{
    std::vector<std::pair<String, UInt64>> res;
    MultiGetRequest mg_req;
    mg_req.gets_.resize(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
    {
        mg_req.gets_[i].table = this->table_name;
        mg_req.gets_[i].key = keys[i];
    }

    MultiGetResponse mg_resp;
    auto code = client->MultiGet(mg_req, &mg_resp);
    assertStatus(OperationType::MULTIGET, code, {Errorcode::OK});
    for (auto ele : mg_resp.results)
    {
        if (ele.first == Errorcode::OK)
            res.emplace_back(std::move(ele.second.value), ele.second.version);
        else
            res.emplace_back("", 0);
    }

    return res;
}

void MetastoreByteKVImpl::MultiWrite::addPut(const String &key, const String &value, const String &expected, bool if_not_exists)
{
    PutRequest put_req;
    put_req.table = this->table_name;
    cache_values.emplace_back(std::make_shared<String>(key));
    put_req.key = *cache_values.back();
    cache_values.emplace_back(std::make_shared<String>(value));
    put_req.value = *cache_values.back();
    put_req.if_not_exists = if_not_exists;
    if (!expected.empty())
    {
        cache_values.emplace_back(std::make_shared<String>(expected));
        auto slice = std::make_shared<Slice>(*cache_values.back());
        expected_values.emplace_back(slice);
        put_req.expected_value = slice.get();
    }
    wb_req.AddPut(put_req);
}

void MetastoreByteKVImpl::MultiWrite::addDelete(const String &key, const UInt64 & expected_version)
{
    DeleteRequest del_req;
    del_req.table = this->table_name;
    cache_values.emplace_back(std::make_shared<String>(key));
    del_req.key = *cache_values.back();
    del_req.expected_version = expected_version;
    wb_req.AddDelete(del_req);
}

void MetastoreByteKVImpl::MultiWrite::setCommitTimeout(const UInt32 & timeout_ms)
{
    wb_req.write_timeout_ms = timeout_ms;
}

std::map<int, String> MetastoreByteKVImpl::MultiWrite::collectConflictInfo()
{
    std::map<int, String> res;
    for (size_t i=0; i<wb_resp.puts_.size(); i++)
    {
        if (wb_resp.puts_[i].first == bytekv::sdk::Errorcode::CAS_FAILED)
            res.emplace(i, wb_resp.puts_[i].second.current_value);
    }
    return res;
}

bool MetastoreByteKVImpl::MultiWrite::commit(bool allow_cas_fail)
{
    // do not perform commit if the wb_req is empty. bytekv will throw `bad request` exception
    if (wb_req.deletes_.empty() && wb_req.puts_.empty())
        return true;

    Errorcode code{};
    if (with_cas)
    {
        code = client->WriteBatch(wb_req, &wb_resp);
        if (allow_cas_fail)
            assertStatus(OperationType::WRITEBATCH, code, {Errorcode::OK, Errorcode::CAS_FAILED});
        else
            assertStatus(OperationType::WRITEBATCH, code, {Errorcode::OK});
    }
    else
    {
        code = client->MultiWrite(wb_req, &wb_resp);
        assertStatus(OperationType::MULTIWRITE, code, {Errorcode::OK});
        /// return code is OK cannot ensure all KV records be serted successfully in MultiWrite, we need check the response.
        for (size_t i=0; i<wb_resp.puts_.size(); i++)
        {
            const auto & [put_code, put_resp] = wb_resp.puts_[i];
            if (put_code != Errorcode::OK)
                throw Exception("Encounter bytekv error: " + String(ErrorString(put_code)) + " while writing record with key : " + wb_req.puts_[i].key.ToString(), put_code);
        }

        for (size_t i=0; i<wb_resp.deletes_.size(); i++)
        {
            const auto & [del_code, del_resp] = wb_resp.deletes_[i];
            if (del_code != Errorcode::OK)
                throw Exception("Encounter bytekv error: " + String(ErrorString(del_code)) + " while deleteing record with key : " + wb_req.deletes_[i].key.ToString(), del_code);
        }
    }
    return code == Errorcode::OK;
}

MultiWritePtr MetastoreByteKVImpl::createMultiWrite(bool with_cas)
{
    return std::make_shared<MultiWrite>(table_name, client, with_cas);
}

bool MetastoreByteKVImpl::multiWriteCAS(const WriteRequests & requests)
{
    WriteBatchRequest wb_req;
    WriteBatchResponse wb_resp;

    std::vector<Slice> expected_values;

    for (auto & req : requests)
    {
        PutRequest put_req;
        put_req.table = this->table_name;
        put_req.key = Slice(req.key.data(), req.key.size());
        put_req.value = Slice(req.new_value.data(), req.new_value.size());
        put_req.if_not_exists = req.if_not_exists;

        if (req.old_value)
        {
            expected_values.push_back(Slice(req.old_value->data(), req.old_value->size()));
            put_req.expected_value = &expected_values.back();
        }

        wb_req.AddPut(put_req);
    }

    auto code = client->WriteBatch(wb_req, &wb_resp);

    if (requests.size() != wb_resp.puts_.size())
        throw Exception("Wrong response size in multiWriteCAS. ", ErrorCodes::METASTORE_OPERATION_ERROR);

    for (size_t i = 0; i < requests.size(); ++i)
    {
        if (auto c = wb_resp.puts_[i].first; requests[i].callback)
            requests[i].callback(int(c), ErrorString(c));
    }

    assertStatus(OperationType::MULTIPUTCAS, code, {Errorcode::OK, Errorcode::CAS_FAILED});
    return code == Errorcode::OK;
}

void MetastoreByteKVImpl::multiPutCAS(const Strings & keys, const String & value_to_insert, const Strings & old_values, bool if_not_exists, std::vector<std::pair<uint32_t , String>> & cas_failed)
{
    ///sanity check
    if (!if_not_exists && keys.size() != old_values.size())
        throw Exception("Wrong arguments.", ErrorCodes::METASTORE_OPERATION_ERROR);

    WriteBatchRequest wb_req;
    WriteBatchResponse wb_resp;

    std::vector<Slice> expected_values;

    if (!if_not_exists)
        expected_values.reserve(keys.size());

    for (size_t i=0; i<keys.size(); i++)
    {
        PutRequest put_req;
        PutResponse put_resp;
        put_req.table = this->table_name;
        put_req.key =  keys[i];
        put_req.value = value_to_insert;
        if (if_not_exists)
            put_req.if_not_exists = true;
        else
        {
            expected_values[i] = Slice(old_values[i]);
            put_req.expected_value = &expected_values[i];
        }

        wb_req.AddPut(put_req);
    }

    auto code = client->WriteBatch(wb_req, &wb_resp);
    assertStatus(OperationType::MULTIPUTCAS, code, {Errorcode::OK, Errorcode::CAS_FAILED});

    if (keys.size() != wb_resp.puts_.size())
        throw Exception("Wrong response size in multiPutCAS. ", ErrorCodes::METASTORE_OPERATION_ERROR);

    if (code == Errorcode::CAS_FAILED)
    {
        for (size_t i=0; i < wb_resp.puts_.size(); i++)
        {
            PutResponse & put_response = std::get<1>(wb_resp.puts_[i]);

            if (if_not_exists)
            {
                if (put_response.current_version!=0)
                    cas_failed.emplace_back(i, put_response.current_value);
            }
            else if (put_response.current_value != old_values[i])
                cas_failed.emplace_back(i, put_response.current_value);
        }
    }
}

void MetastoreByteKVImpl::update(const String& key, const String& value)
{
    ///bytekv will write a new version. we may need version control here.
    put(key, value);
}

void MetastoreByteKVImpl::drop(const String & key, const UInt64 & expected_version)
{
    DeleteRequest del_req;
    DeleteResponse del_resp;
    del_req.table = this->table_name;
    del_req.key = key;
    del_req.expected_version = expected_version;
    auto code = client->Delete(del_req, &del_resp);
    assertStatus(OperationType::DELETE, code, {Errorcode::OK});
}

MetastoreByteKVImpl::IteratorPtr MetastoreByteKVImpl::getAll()
{
    return getByPrefix("");
}

MetastoreByteKVImpl::IteratorPtr MetastoreByteKVImpl::getByPrefix(const String & partition_id, const size_t & limit, uint32_t scan_batch_size)
{
    ScanRequest scan_req;
    scan_req.scan_batch_count = scan_batch_size;
    scan_req.limit = limit;
    scan_req.table = table_name;
    scan_req.start_key = partition_id;
    String end_key = getNextKey(partition_id);
    scan_req.end_key = end_key;
    ScanResponse scan_resp;

    auto code = client->Scan(scan_req, &scan_resp);
    assertStatus(OperationType::SCAN, code, {Errorcode::OK});

    return std::make_shared<ByteKVIterator>(scan_resp.iterator);
}

MetastoreByteKVImpl::IteratorPtr MetastoreByteKVImpl::getByRange(const String & range_start, const String & range_end, const bool include_start, const bool include_end)
{
    ScanRequest scan_req;
    ScanResponse scan_resp;

    String start_key = include_start ? range_start : getNextKey(range_start);
    String end_key = include_end ? getNextKey(range_end) : range_end;

    scan_req.scan_batch_count = DEFAULT_SCAN_BATCH_COUNT;
    scan_req.table = table_name;
    scan_req.start_key = start_key;
    scan_req.end_key = end_key;

    auto code = client->Scan(scan_req, &scan_resp);
    assertStatus(OperationType::SCAN, code, {Errorcode::OK});

    return std::make_shared<ByteKVIterator>(scan_resp.iterator);
}

void MetastoreByteKVImpl::clean(const String & prefix)
{
    auto batch_delete = [&](std::vector<String> & keys)
    {
        WriteBatchRequest wb_req;
        wb_req.deletes_.resize(keys.size());
        for (size_t i=0; i < keys.size(); i++)
        {
            auto & del_req = wb_req.deletes_[i];
            del_req.table = this->table_name;
            del_req.key = keys[i];
        }

        WriteBatchResponse wb_resp;
        auto code = client->WriteBatch(wb_req, &wb_resp);
        assertStatus(OperationType::CLEAN, code, {Errorcode::OK});
    };

    IteratorPtr it = getByPrefix(prefix);

    std::vector<String> to_be_deleted;

    while(it->next())
    {
        to_be_deleted.emplace_back(it->key());

        /// safe guard. avoid too big write.
        if (to_be_deleted.size() == MAX_BATCH_SIZE)
        {
            batch_delete(to_be_deleted);
            to_be_deleted.clear();
        }
    }

    if (!to_be_deleted.empty())
        batch_delete(to_be_deleted);
}

void MetastoreByteKVImpl::cleanAll()
{
    ScanRequest scan_req;
    scan_req.scan_batch_count = DEFAULT_SCAN_BATCH_COUNT;
    scan_req.table = table_name;
    scan_req.start_key = "";
    ScanResponse scan_resp;

    auto code = client->Scan(scan_req, &scan_resp);
    assertStatus(OperationType::SCAN, code, {Errorcode::OK});

    IteratorPtr  it= std::make_shared<ByteKVIterator>(scan_resp.iterator);

    auto batch_delete = [&](std::vector<String> & keys)
    {
        WriteBatchRequest wb_req;
        for (size_t i=0; i<keys.size(); i++)
        {
            DeleteRequest del_req;
            del_req.table = this->table_name;
            del_req.key = keys[i];
            wb_req.AddDelete(del_req);
        }

        WriteBatchResponse wb_resp;
        code = client->WriteBatch(wb_req, &wb_resp);
        assertStatus(OperationType::CLEAN, code, {Errorcode::OK});
    };

    std::vector<String> to_be_deleted;

    while(it->next())
    {
        String key = it->key();
        to_be_deleted.emplace_back(std::move(key));

        /// safe guard. avoid too big write.
        if (to_be_deleted.size() == MAX_BATCH_SIZE)
        {
            batch_delete(to_be_deleted);
            to_be_deleted.clear();
        }
    }

    if (!to_be_deleted.empty())
        batch_delete(to_be_deleted);

}

void MetastoreByteKVImpl::assertStatus(const OperationType & op, const Errorcode & code, const ExpectedCodes & expected)
{
    for (auto expected_code : expected)
    {
        if (expected_code == code)
            return;
    }
    throw Exception("Unexpected result from byteKV. Operation : " + Operation(op) + ", Errormsg : " + ErrorString(code) , toCommonErrorCode(code));
}

int MetastoreByteKVImpl::toCommonErrorCode(const Errorcode & code)
{
    switch (code)
    {
    case Errorcode::CAS_FAILED :
        return ErrorCodes::METASTORE_COMMIT_CAS_FAILURE;
    
    default:
        return code;
    }
}


}
}
