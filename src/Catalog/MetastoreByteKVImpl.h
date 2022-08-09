#pragma once

#include <Catalog/IMetastore.h>
#include <bytekv4cpp/bytekv/client.h>
#include <Catalog/MetaStoreOperations.h>
#include <Common/Exception.h>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int METASTORE_OPERATION_ERROR;
    extern const int METASTORE_EXCEPTION;
}

namespace Catalog
{

using namespace bytekv::sdk;

class MetastoreByteKVImpl : public IMetaStore
{

public:
    using ExpectedCodes = std::initializer_list<Errorcode>;

    struct ByteKVIterator: public IMetaStore::Iterator
    {
    public:
        ByteKVIterator(std::shared_ptr<bytekv::sdk::Iterator> & it) : inner_it(it) {}

        ~ByteKVIterator() override = default;

        inline bool next() override
        {
            bool has_next = inner_it->Next(&code);
            if (!has_next && code != Errorcode::ITERATOR_END)
                throw Exception("ByteKV iterator not end correctly, " + String(ErrorString(code)) + ". Reason : " + inner_it->ErrorText(), ErrorCodes::METASTORE_EXCEPTION);
            return has_next;
        }

        inline String key() override
        {
            return inner_it->Key();
        }

        inline String value() override
        {
            return inner_it->Value();
        }
    private:
        Errorcode code;
        std::shared_ptr<bytekv::sdk::Iterator> inner_it;
    };

    class MultiWrite : public IMultiWrite
    {
    public:
        MultiWrite(const String & table_name_, std::shared_ptr<ByteKVClient> client_, bool with_cas_ = true)
            :table_name(table_name_), client(client_), with_cas(with_cas_) {}
        void addPut(const String & key, const String & value, const String & expected = "", bool if_not_exists = false) override;
        void addDelete(const String & key, const UInt64 & expected_version = 0) override;
        void setCommitTimeout(const UInt32 & timeout_ms) override;
        bool commit(bool allow_cas_fail = false) override;
        std::map<int, String> collectConflictInfo() override;
        inline size_t getPutsSize() override { return wb_req.puts_.size(); }
        inline size_t getDeleteSize() override{ return  wb_req.deletes_.size(); }
        inline bool isEmpty() override { return wb_req.puts_.empty() && wb_req.deletes_.empty(); }
        ~MultiWrite() override {}
    private:
        String table_name;
        std::shared_ptr<ByteKVClient> client;
        bool with_cas;
        std::vector<std::shared_ptr<String>> cache_values;
        std::vector<std::shared_ptr<Slice>> expected_values;
        WriteBatchRequest wb_req;
        WriteBatchResponse wb_resp;
    };

    MetastoreByteKVImpl(const String & service_name_, const String & cluster_name_,
                        const String & name_space_, const String & table_name_);


    MultiWritePtr createMultiWrite(bool with_cas = true) override;

    void init();

    void put(const String & key, const String & value, bool if_not_exists = false) override;

    void putTTL(const String & key, const String & value, UInt64 ttl);

    bool putCAS(const String & key, const String & value, const String & expected) override;

    // return old value if cas failed
    std::pair<bool, String> putCASWithOldValue(const String & key, const String & value, const String & expected) override;

    uint64_t get(const String & key, String & value) override;

    std::vector<std::pair<String, UInt64>> multiGet(const std::vector<String> & keys) override;

    void multiPutCAS(const Strings & keys, const String & value_to_insert, const Strings & old_values,
                     bool if_not_exists, std::vector<std::pair<uint32_t , String>> & cas_failed) override;

    bool multiWriteCAS(const WriteRequests & requests) override;

    void update(const String& key, const String& value) override;

    void drop(const String & key, const UInt64 & expected_version = 0) override;

    IteratorPtr getAll() override;

    IteratorPtr getByPrefix(const String & partition_id, const size_t & limit = 0, uint32_t scan_batch_size = DEFAULT_SCAN_BATCH_COUNT) override;

    IteratorPtr getByRange(const String & range_start, const String & range_end, const bool include_start, const bool include_end) override;

    void clean(const String & prefix) override;

    ///only for test;
    void cleanAll();

    void close() override {}

    static void assertStatus(const OperationType & op, const Errorcode & code, const ExpectedCodes & expected);

public:
    std::shared_ptr<ByteKVClient> client;


private:

    String service_name;
    String cluster_name;
    String name_space;
    String table_name;

    /// convert metastore specific error code to Clickhouse error code for processing convenience in upper layer.
    static int toCommonErrorCode(const Errorcode & code);
};
}


}
