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

#include <cstdint>
#include <string>
#include <string.h>
#include <Catalog/FDBClient.h>
#include <Catalog/FDBError.h>
#include <Common/Exception.h>
#include <Catalog/MetastoreFDBImpl.h>
#include <common/logger_useful.h>


namespace DB
{

constexpr int64_t default_transaction_timeout = 5000; ///5 seconds
namespace ErrorCodes
{
    extern const int METASTORE_EXCEPTION;
}

namespace FDB
{

#define RETURN_ON_ERROR(code) \
    if (code) \
        return code

#define THROW_ON_ERROR(code) \
    if (code) \
        throw Exception("Error occurs while initializing fdb client. code: " + std::to_string(code) + ", msg: " \
                + std::string(fdb_get_error(code)), ErrorCodes::METASTORE_EXCEPTION)

static void AssertTrsansactionStatus(FDBTransactionPtr tr)
{
    if (!tr || !tr->transaction)
        throw Exception("Transaction is not initialized.", ErrorCodes::METASTORE_EXCEPTION);
}

static fdb_error_t waitFuture(FDBFuture * future)
{
    fdb_error_t block_error = fdb_future_block_until_ready(future);
    if (!block_error)
		return fdb_future_get_error(future);
	else
		return block_error;
}

/// automatically retry transaction if encounter retryable error
template<typename Runnable>
static fdb_error_t RunWithRetry(FDBTransactionPtr tr, size_t max_retry, Runnable && op)
{
    fdb_error_t code = 0;
    max_retry = max_retry > 0 ? max_retry : 1;
    while (max_retry--)
    {
        code = op();
        // avoid retry if return error code is `not_committed` (caused by conflicts)
        if (code == 0 || code == FDBError::FDB_not_committed)
            return code;
        else
        {
            FDBFuturePtr f = std::make_shared<FDBFutureRAII>(fdb_transaction_on_error(tr->transaction, code));
            if (fdb_error_t f_code = waitFuture(f->future); f_code)
                return code;
            // continue the loop and perform the operation again.
            LOG_WARNING(getLogger("FDBClient::RunWithRetry"), "Try perform the transaction again with retryable error : {}, remain retry time: {}",
                std::string(fdb_get_error(code)), max_retry);
        }
    }
    return code;
}

FDBTransactionRAII::FDBTransactionRAII()
{
}

FDBTransactionRAII::FDBTransactionRAII(FDBTransaction * tr_)
    : transaction(std::move(tr_))
{
}

FDBTransactionRAII::~FDBTransactionRAII()
{
    if (transaction)
        fdb_transaction_destroy(transaction);
}

FDBFutureRAII::FDBFutureRAII(FDBFuture * future_)
    : future(std::move(future_))
{
}

FDBFutureRAII::~FDBFutureRAII()
{
    if (future)
        fdb_future_destroy(future);
}
FDBClientPtr FDBClient::Instance(const std::string & cluster_file)
{
    static FDBClientPtr client_ptr = FDBClientPtr(new FDBClient(cluster_file));
    return client_ptr;
}


FDBClient::FDBClient(const std::string & cluster_file)
{
    THROW_ON_ERROR(fdb_select_api_version(710));
    THROW_ON_ERROR(fdb_setup_network());
    pthread_create(&fdb_netThread, nullptr, [](void* )->void * {THROW_ON_ERROR(fdb_run_network()); return nullptr;}, nullptr);
    THROW_ON_ERROR(fdb_create_database(cluster_file.c_str(), &fdb));
    THROW_ON_ERROR(fdb_database_set_option(fdb, FDB_DB_OPTION_TRANSACTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&default_transaction_timeout), sizeof(default_transaction_timeout)));
    int64_t max_delay_ms = 200;
    THROW_ON_ERROR(fdb_database_set_option(fdb, FDBDatabaseOption::FDB_DB_OPTION_TRANSACTION_MAX_RETRY_DELAY,
        reinterpret_cast<const uint8_t*>(&max_delay_ms), sizeof(max_delay_ms)));
}

FDBClient::~FDBClient()
{
    if (fdb)
        fdb_database_destroy(fdb);
    [[maybe_unused]]auto code = fdb_stop_network();
    pthread_join(fdb_netThread, nullptr);
}

fdb_error_t FDBClient::CreateTransaction(FDBTransactionPtr tr)
{
    RETURN_ON_ERROR(fdb_database_create_transaction(fdb, &(tr->transaction)));
    return 0;
}

fdb_error_t FDBClient::Get(FDBTransactionPtr tr, const std::string & key, GetResponse & res)
{
    AssertTrsansactionStatus(tr);
    const uint8_t* p_key = reinterpret_cast<const uint8_t*>(key.c_str());
    FDBFuturePtr f_read = std::make_shared<FDBFutureRAII>(fdb_transaction_get(tr->transaction, p_key, key.size(), 1));
    RETURN_ON_ERROR(waitFuture(f_read->future));
    fdb_bool_t present;
    uint8_t const *outValue;
    int outValueLength;
    RETURN_ON_ERROR(fdb_future_get_value(f_read->future, &present, &outValue, &outValueLength));
    if (present)
    {
        res.is_present = true;
        res.value = std::string(outValue, outValue+outValueLength);
    }

    return 0;
}

fdb_error_t FDBClient::Put(FDBTransactionPtr tr, const PutRequest & put)
{
    return RunWithRetry(tr, FDB_DEFAULT_MAX_RETRY, [&]()
    {
        AssertTrsansactionStatus(tr);
        if (put.if_not_exists || put.expected_value)
        {
            GetResponse get_res;
            Get(tr, std::string(put.key.data, put.key.size), get_res);
            if ((put.if_not_exists && get_res.is_present)
                || (put.expected_value && std::string_view(put.expected_value.value()) != get_res.value))
                return static_cast<fdb_error_t>(FDBError::FDB_not_committed);
        }
        fdb_transaction_set(tr->transaction, reinterpret_cast<const uint8_t*>(put.key.data), put.key.size, reinterpret_cast<const uint8_t*>(put.value.data), put.value.size);
        FDBFuturePtr f = std::make_shared<FDBFutureRAII>(fdb_transaction_commit(tr->transaction));
        return waitFuture(f->future);
    });
}

std::shared_ptr<Iterator> FDBClient::Scan(FDBTransactionPtr tr, const ScanRequest & scan_req)
{
    AssertTrsansactionStatus(tr);
    return std::make_shared<Iterator>(this, tr, scan_req);
}

/*
values: <string, UInt64> pair, the string is the value string and UInt64 is the length of the value string.
*/
fdb_error_t FDBClient::MultiGet(FDBTransactionPtr tr, const std::vector<std::string> & keys, std::vector<std::pair<std::string, UInt64>> & values)
{
    if (keys.empty())
        return 0;
    AssertTrsansactionStatus(tr);
    std::vector<FDBFuturePtr> future_list;
    for (auto & key : keys)
    {
        const uint8_t* p_key = reinterpret_cast<const uint8_t*>(key.c_str());
        FDBFuturePtr f_read = std::make_shared<FDBFutureRAII>(fdb_transaction_get(tr->transaction, p_key, key.size(), 1));
        future_list.emplace_back(f_read);
    }

    fdb_bool_t present;
    uint8_t const *outValue;
    int outValueLength;
    for (FDBFuturePtr f_read : future_list)
    {
        RETURN_ON_ERROR(fdb_future_block_until_ready(f_read->future));
        RETURN_ON_ERROR(fdb_future_get_value(f_read->future, &present, &outValue, &outValueLength));
        if (present)
            values.emplace_back(std::make_pair(std::string(outValue, outValue+outValueLength), outValueLength));
        else
            values.emplace_back(std::make_pair("", 0));
    }

    return 0;
}

fdb_error_t FDBClient::MultiWrite(FDBTransactionPtr tr, const Catalog::BatchCommitRequest & req, Catalog::BatchCommitResponse & resp)
{
    if (req.isEmpty())
        return 0;
    if (req.commit_timeout_ms)
    {
        uint64_t timeout = req.commit_timeout_ms;
        THROW_ON_ERROR(fdb_transaction_set_option(tr->transaction, FDBTransactionOption::FDB_TR_OPTION_TIMEOUT,
            reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));
    }
    size_t retry_times = req.commit_timeout_ms ? 1 : FDB_DEFAULT_MAX_RETRY;
    return RunWithRetry(tr, retry_times, [&](){
        AssertTrsansactionStatus(tr);

        resp.reset();
        std::vector<int> index_of_cas_req;
        std::vector<FDBFuturePtr> future_list;

        for (size_t i = 0; i < req.puts.size(); ++i)
        {
            const Catalog::SinglePutRequest & single_put_req = req.puts[i];
            if (single_put_req.if_not_exists || single_put_req.expected_value)
            {
                const uint8_t* p_key = reinterpret_cast<const uint8_t*>(single_put_req.key.data());
                FDBFuturePtr f_read = std::make_shared<FDBFutureRAII>(fdb_transaction_get(tr->transaction, p_key, single_put_req.key.size(), 0));
                future_list.emplace_back(f_read);
                index_of_cas_req.emplace_back(i);
            }
        }
        size_t puts_cas_size = index_of_cas_req.size();

        for (size_t i = 0; i < req.deletes.size(); ++i)
        {
            const Catalog::SingleDeleteRequest & single_del_req = req.deletes[i];
            if (single_del_req.expected_value)
            {
                const uint8_t* p_key = reinterpret_cast<const uint8_t*>(single_del_req.key.data());
                FDBFuturePtr f_read = std::make_shared<FDBFutureRAII>(fdb_transaction_get(tr->transaction, p_key, single_del_req.key.size(), 0));
                future_list.emplace_back(f_read);
                index_of_cas_req.emplace_back(i);
            }
        }
        size_t deletes_cas_size = index_of_cas_req.size() - puts_cas_size;

        fdb_bool_t present;
        uint8_t const *outValue;
        int outValueLength;

        // check for puts
        for (size_t i = 0; i < puts_cas_size; ++i)
        {
            RETURN_ON_ERROR(waitFuture(future_list[i]->future));
            RETURN_ON_ERROR(fdb_future_get_value(future_list[i]->future, &present, &outValue, &outValueLength));

            const Catalog::SinglePutRequest & put_req = req.puts[index_of_cas_req[i]];

            if (put_req.if_not_exists)
            {
                if (present)
                    resp.puts.emplace(index_of_cas_req[i], std::string(outValue, outValue+outValueLength));
            }
            else if (put_req.expected_value)
            {
                if (present)
                {
                    if (put_req.expected_value->size() != static_cast<size_t>(outValueLength) || memcmp(put_req.expected_value->data(), outValue, outValueLength))
                        resp.puts.emplace(index_of_cas_req[i], std::string(outValue, outValue + outValueLength));
                }
                else
                {
                    resp.puts.emplace(index_of_cas_req[i], "");
                }
            }
        }

        // check for deletes
        for (size_t i = puts_cas_size; i < puts_cas_size + deletes_cas_size; ++i)
        {
            RETURN_ON_ERROR(waitFuture(future_list[i]->future));
            RETURN_ON_ERROR(fdb_future_get_value(future_list[i]->future, &present, &outValue, &outValueLength));

            const Catalog::SingleDeleteRequest & del_req = req.deletes[index_of_cas_req[i]];

            if (del_req.expected_value)
            {
                if (present)
                {
                    if (del_req.expected_value->size() != static_cast<size_t>(outValueLength) || memcmp(del_req.expected_value->data(), outValue, outValueLength))
                        resp.deletes.emplace(index_of_cas_req[i], std::string(outValue, outValue + outValueLength));
                }
                else
                {
                    resp.deletes.emplace(index_of_cas_req[i], "");
                }
            }
        }

        /// return immediately if find any conflict in current commit;
        if (!resp.puts.empty() || !resp.deletes.empty())
            return static_cast<fdb_error_t>(FDBError::FDB_not_committed);

        for (size_t i = 0; i < req.puts.size(); ++i)
        {
            fdb_transaction_set(tr->transaction,
                reinterpret_cast<const uint8_t*>(req.puts[i].key.data()),
                req.puts[i].key.size(),
                reinterpret_cast<const uint8_t*>(req.puts[i].value.data()),
                req.puts[i].value.size()
            );
        }

        for (size_t i = 0; i < req.deletes.size(); ++i)
            fdb_transaction_clear(tr->transaction, reinterpret_cast<const uint8_t*>(req.deletes[i].key.c_str()), req.deletes[i].key.size());

        FDBFuturePtr f = std::make_shared<FDBFutureRAII>(fdb_transaction_commit(tr->transaction));
        RETURN_ON_ERROR(fdb_future_block_until_ready(f->future));
        fdb_error_t error_code = fdb_future_get_error(f->future);

        /// The commit would still fail due to conflict (CAS), so we need to read for the conflicted value again.
        /// Note, the values conflict here may not be the same as the values in the conflict commit because FDB doesn't support CAS operations.
        /// So from the caller side, cannot have the above assumption, only can guarantee they are latest value for read this time,
        /// should perform further operations in CAS way.
        if (error_code == FDBError::FDB_not_committed)
        {
            // handle error, after this, the transation will be reset and we can use it to get conflict keys
            FDBFuturePtr f_error = std::make_shared<FDBFutureRAII>(fdb_transaction_on_error(tr->transaction, error_code));
            THROW_ON_ERROR(waitFuture(f_error->future));

            std::vector<std::string> req_keys;
            std::vector<std::string> req_values;
            for (size_t i = 0; i < req.puts.size(); ++i)
            {
                const Catalog::SinglePutRequest & single_put_req = req.puts[i];
                if (single_put_req.if_not_exists || single_put_req.expected_value)
                {
                    req_keys.push_back(single_put_req.key);
                    req_values.push_back(single_put_req.expected_value.value_or(""));
                }
            }

            for (size_t i = 0; i < req.deletes.size(); ++i)
            {
                const Catalog::SingleDeleteRequest & single_del_req = req.deletes[i];
                if (single_del_req.expected_value)
                {
                    req_keys.push_back(single_del_req.key);
                    req_values.push_back(single_del_req.expected_value.value_or(""));
                }
            }

            // snapshot multi get, res: <value, length> pair
            // after txn conflict, the values may vary, the following logic for the values gain here should also use CAS operation.
            std::vector<std::pair<String, UInt64>> res;

            THROW_ON_ERROR(MultiGet(tr, req_keys, res));

            for (size_t i = 0; i < puts_cas_size; ++i)
            {
                if ((req_values[i].size() != res[i].second) || memcmp(req_values[i].data(), res[i].first.data(), res[i].second))
                {
                    resp.puts.emplace(index_of_cas_req[i], res[i].first);
                }
            }

            for (size_t i = puts_cas_size; i < puts_cas_size + deletes_cas_size; ++i)
            {
                if ((req_values[i].size() != res[i].second) || memcmp(req_values[i].data(), res[i].first.data(), res[i].second))
                {
                    resp.deletes.emplace(index_of_cas_req[i], res[i].first);
                }
            }
        }

        return error_code;
    });
}

fdb_error_t FDBClient::Delete(FDBTransactionPtr tr, const std::string & key, const std::string & expected)
{
    return RunWithRetry(tr, FDB_DEFAULT_MAX_RETRY, [&](){
        AssertTrsansactionStatus(tr);
        if (!expected.empty())
        {
            FDBFuturePtr f_read = std::make_shared<FDBFutureRAII>(fdb_transaction_get(tr->transaction, reinterpret_cast<const uint8_t*>(key.c_str()), key.size(), 0));
            RETURN_ON_ERROR(waitFuture(f_read->future));
            fdb_bool_t present;
            uint8_t const *outValue;
            int outValueLength;
            RETURN_ON_ERROR(fdb_future_get_value(f_read->future, &present, &outValue, &outValueLength));

            if (!present || (expected.size() != static_cast<size_t>(outValueLength) || memcmp(expected.data(), outValue, outValueLength)))
                return static_cast<fdb_error_t>(FDBError::FDB_not_committed);
        }
        fdb_transaction_clear(tr->transaction, reinterpret_cast<const uint8_t*>(key.c_str()), key.size());
        FDBFuturePtr f = std::make_shared<FDBFutureRAII>(fdb_transaction_commit(tr->transaction));
        return waitFuture(f->future);
    });
}

fdb_error_t FDBClient::Clear(FDBTransactionPtr tr, const std::string & start_key, const std::string & end_key)
{
    AssertTrsansactionStatus(tr);
    fdb_transaction_clear_range(tr->transaction, reinterpret_cast<const uint8_t*>(start_key.c_str()), start_key.size(), reinterpret_cast<const uint8_t*>(end_key.c_str()), end_key.size());
    FDBFuturePtr f = std::make_shared<FDBFutureRAII>(fdb_transaction_commit(tr->transaction));
    return waitFuture(f->future);
}

void FDBClient::DestroyTransaction(FDBTransactionPtr tr)
{
    AssertTrsansactionStatus(tr);
    fdb_transaction_destroy(tr->transaction);
}

Iterator::Iterator(FDBClient * client_, FDBTransactionPtr tr_, const ScanRequest & req_)
    : client{client_}, tr(tr_), req(req_)
{
    AssertTrsansactionStatus(tr);
    start_key_batch = req.start_key;
}

bool Iterator::Next(fdb_error_t & code)
{
    if (batch_count > batch_read_index)
    {
        batch_read_index++;
        read_count++;
        return true;
    }
    else
    {
        if (iteration > 1 && ((read_count == req.row_limit) || !has_more))
            return false;

        while (true)
        {
            if (iteration==1)
            {
                if (req.exclude_start_key)
                {
                    batch_future = std::make_shared<FDBFutureRAII>(fdb_transaction_get_range(
                        tr->transaction,
                        FDB_KEYSEL_FIRST_GREATER_THAN(reinterpret_cast<const uint8_t *>(start_key_batch.c_str()), start_key_batch.size()),
                        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(reinterpret_cast<const uint8_t *>(req.end_key.c_str()), req.end_key.size()),
                        req.row_limit,
                        0,
                        FDB_STREAMING_MODE_LARGE,
                        iteration,
                        1,
                        req.reverse_order));
                }
                else
                {
                    batch_future = std::make_shared<FDBFutureRAII>(fdb_transaction_get_range(
                        tr->transaction,
                        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(
                            reinterpret_cast<const uint8_t *>(start_key_batch.c_str()), start_key_batch.size()),
                        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(reinterpret_cast<const uint8_t *>(req.end_key.c_str()), req.end_key.size()),
                        req.row_limit,
                        0,
                        FDB_STREAMING_MODE_LARGE,
                        iteration,
                        1,
                        req.reverse_order));
                }
            }
            else
            {
                batch_future = std::make_shared<FDBFutureRAII>(fdb_transaction_get_range(
                    tr->transaction,
                    FDB_KEYSEL_FIRST_GREATER_THAN(reinterpret_cast<const uint8_t *>(start_key_batch.c_str()), start_key_batch.size()),
                    FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(reinterpret_cast<const uint8_t *>(req.end_key.c_str()), req.end_key.size()),
                    std::min(req.row_limit, req.row_limit - read_count),
                    0,
                    FDB_STREAMING_MODE_LARGE,
                    iteration,
                    1,
                    req.reverse_order));
            }

            code = waitFuture(batch_future->future);
            if (code == FDBError::FDB_transaction_too_old
                || code == FDBError::FDB_transaction_timed_out)
            {
                LOG_DEBUG(getLogger("FDBIterator"), "Transaction timeout or too old, create new transaction");
                tr = std::make_shared<FDB::FDBTransactionRAII>();
                Catalog::MetastoreFDBImpl::check_fdb_op(client->CreateTransaction(tr));
                continue;
            }
            else if (code)
                return false;

            break;
        }

        if (code = fdb_future_get_keyvalue_array(batch_future->future, &batch_kvs, &batch_count, &has_more); code)
            return false;

        if (batch_count > 0)
        {
            start_key_batch = std::string(reinterpret_cast<const char *>(batch_kvs[batch_count-1].key), batch_kvs[batch_count-1].key_length);
            iteration++;
            batch_read_index = 1;
            read_count++;
            return true;
        }
        else
            return false;
    }
}

std::string Iterator::Key()
{
    return std::string(reinterpret_cast<const char *>(batch_kvs[batch_read_index-1].key), batch_kvs[batch_read_index-1].key_length);
}

std::string Iterator::Value()
{
    return std::string(reinterpret_cast<const char *>(batch_kvs[batch_read_index-1].value), batch_kvs[batch_read_index-1].value_length);
}

}
}
