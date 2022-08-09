#pragma once

#include <Catalog/IMetastore.h>
#include <Catalog/FDBClient.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int METASTORE_EXCEPTION;
}

namespace Catalog
{

class MetastoreFDBImpl : public IMetaStore
{
public:
    struct FDBIterator: public IMetaStore::Iterator
    {
    public:
        FDBIterator(std::shared_ptr<FDB::Iterator> iter)
            : inner_iter(iter)
        {
        }

        ~FDBIterator() override = default;

        inline bool next() override
        {
            bool has_next = inner_iter->Next(error_code);
            if (error_code)
                throw Exception("Error whiling scanning data through FBDItrator. message: " + String(fdb_get_error(error_code)), ErrorCodes::METASTORE_EXCEPTION);
            return has_next;
        }

        inline String key() override
        {
            return inner_iter->Key();
        }

        inline String value() override
        {
            return inner_iter->Value();
        }

    private:
        std::shared_ptr<FDB::Iterator> inner_iter;
        fdb_error_t error_code = 0;
    };

    class MultiWrite : public IMultiWrite
    {
    public:
        MultiWrite(FDB::FDBClientPtr client) : fdb_client(client) {}
        void addPut(const String & key, const String & value, const String & expected = "", bool if_not_exists = false) override;
        void addDelete(const String & , [[maybe_unused]]const UInt64 & expected_version = 0) override;
        inline bool isEmpty() override {return w_req.puts_.empty() && w_req.deletes_.empty(); }
        bool commit(bool allow_cas_fail = true) override;
        void setCommitTimeout(const UInt32 & /*timeout_ms*/) override {}
        inline size_t getPutsSize() override { return w_req.puts_.size(); }
        inline size_t getDeleteSize() override { return  w_req.deletes_.size(); }
        std::map<int, String> collectConflictInfo() override {return {};}
        ~MultiWrite() override {}

    private:
        FDB::FDBClientPtr fdb_client = nullptr;
        std::vector<std::shared_ptr<String>> cached_values;
        FDB::MultiWriteRequest w_req;
        FDB::MultiWriteResponse w_resp;
    };

    MetastoreFDBImpl(const String & cluster_config_path);

    MultiWritePtr createMultiWrite(bool with_cas = false) override;

    void put(const String & key, const String & value, bool if_not_exists = false) override;

    bool putCAS(const String & key, const String & value, const String & expected) override;

    std::pair<bool, String> putCASWithOldValue(const String & key, const String & value, const String & expected) override;

    uint64_t get(const String & key, String & value) override;

    std::vector<std::pair<String, UInt64>> multiGet(const std::vector<String> & keys) override;

    void multiPutCAS(const Strings & keys, const String & value_to_insert, const Strings & old_values,
                     bool if_not_exists, std::vector<std::pair<uint32_t , String>> & cas_failed) override;

    bool multiWriteCAS(const WriteRequests & requests) override;

    void update(const String& key, const String& value) override;

    void drop(const String &, const UInt64 & expected = 0) override;

    IteratorPtr getAll() override;

    IteratorPtr getByPrefix(const String &, const size_t & limit = 0, uint32_t scan_batch_size = DEFAULT_SCAN_BATCH_COUNT) override;

    IteratorPtr getByRange(const String & range_start, const String & range_end, const bool include_start, const bool include_end) override;

    void clean(const String & prefix) override;

    void close() override {}

private:
    static void check_fdb_op(const fdb_error_t & error_t);
    /// convert metastore specific error code to Clickhouse error code for processing convenience in upper layer.
    static int toCommonErrorCode(const fdb_error_t & error_t);
    
    FDB::FDBClientPtr fdb_client;
};

}


}
