#pragma once
#include <memory>
#include <common/StringRef.h>
#include <pthread.h>

#ifndef FDB_API_VERSION
#define FDB_API_VERSION 710
#endif
#include <foundationdb/fdb_c.h>
#include <optional>

/***
 *  C++ client for fdb
 */ 

namespace DB
{

namespace FDB
{

struct PutRequest
{
    StringRef key;
    StringRef value;
    bool if_not_exists = false;
    std::optional<StringRef> expected_value;
};

struct GetResponse
{
    bool is_present {false};
    std::string value;
};

struct MultiWriteRequest
{
    void AddPut(const PutRequest& put) { puts_.push_back(put); }
    void AddDelete(const std::string& del) { deletes_.push_back(del); }
    std::vector<PutRequest> puts_;
    std::vector<std::string> deletes_;
};


struct MultiWriteResponse
{
    std::vector<std::pair<int, std::string>> puts_;
    std::vector<std::pair<int, std::string>> deletes_;
};

struct ScanRequest
{
    std::string start_key;
    std::string end_key;

    uint32_t row_limit = 20000;
    fdb_bool_t reverse_order = 0;
};

/***
 *  RAII object encapsulating the FDBTransaction
 */
class FDBTransactionRAII
{
public:
    FDBTransaction * transaction = nullptr;
    FDBTransactionRAII();
    FDBTransactionRAII(FDBTransaction * tr_);
    ~FDBTransactionRAII();
};

/***
 *  RAII object encapsulating the FDBFuture
 */
class FDBFutureRAII
{
public:
    FDBFuture * future = nullptr;
    explicit FDBFutureRAII(FDBFuture * future_);
    ~FDBFutureRAII();
};

using FDBTransactionPtr = std::shared_ptr<FDBTransactionRAII>;
using FDBFuturePtr = std::shared_ptr<FDBFutureRAII>;

class Iterator {
public:
    Iterator(FDBTransactionPtr tr_, const ScanRequest & req_);
    bool Next(fdb_error_t & code);
    std::string Key();
    std::string Value();
private:
    FDBTransactionPtr tr = nullptr;
    ScanRequest req;
    FDBFuturePtr batch_future = nullptr;
    std::string start_key_batch = "";
    int iteration = 1;
    int batch_count = 0;
    fdb_bool_t has_more = false;
    const FDBKeyValue *batch_kvs = nullptr;
    int batch_read_index = 0;
};

class FDBClient
{
    friend Iterator;
public:
    FDBClient(const std::string & cluster_file);
    ~FDBClient();
    fdb_error_t CreateTransaction(FDBTransactionPtr tr);
    fdb_error_t Get(FDBTransactionPtr tr, const std::string & key, GetResponse & res);
    fdb_error_t Put(FDBTransactionPtr tr, const PutRequest & put);
    std::shared_ptr<Iterator> Scan(FDBTransactionPtr tr, const ScanRequest & scan_req);
    fdb_error_t MultiGet(FDBTransactionPtr tr, const std::vector<std::string> & keys, std::vector<std::pair<std::string, UInt64>> & values);
    fdb_error_t MultiWrite(FDBTransactionPtr tr, const MultiWriteRequest & req, MultiWriteResponse & resp);
    fdb_error_t Delete(FDBTransactionPtr tr, const std::string & key);
    fdb_error_t Clear(FDBTransactionPtr tr, const std::string & start_key, const std::string & end_key);
    void DestroyTransaction(FDBTransactionPtr tr);

private:
    pthread_t fdb_netThread;
    FDBDatabase * fdb = nullptr;
};

using FDBClientPtr = std::shared_ptr<FDBClient>;

}

}
