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
#include <string>
#include <optional>
#include <vector>
#include <unordered_map>

namespace DB
{

namespace Catalog
{

#define MAX_BATCH_SIZE 1024
#define DEFAULT_SCAN_BATCH_COUNT 10000
#define DEFAULT_MULTI_GET_BATCH_COUNT 100

enum CatalogCode : int
{
    OK = 0,
    KEY_NOT_FOUND = -10002,
    CAS_FAILED = -10003,
};

struct SingleRequest
{
    explicit SingleRequest(const std::string & key_)
        : key(key_)
    {
    }
    SingleRequest(const std::string & key_, const std::string & value_, bool if_not_exists_ = false)
        : key(key_), value(value_), if_not_exists(if_not_exists_)
    {
    }
    SingleRequest(const std::string & key_, const std::string & value_, const std::string & expected_, const uint64_t & expected_version_ = 0)
        : key(key_), value(value_), expected_version(expected_version_)
    {
        if (!expected_.empty())
            expected_value = expected_;
    }
    SingleRequest(const std::string & key_, const std::string & value_, uint64_t ttl_) : key(key_), value(value_), ttl(ttl_)
    {
    }

    bool isEmpty() { return key.empty();}

    uint32_t size() const {return (expected_value ? expected_value->size() : 0) + key.size() + value.size(); }

    std::string key;
    std::string value;
    bool if_not_exists = false;
    std::optional<std::string> expected_value;
    uint64_t expected_version = 0;
    // the inserted key will be expired after n seconds.
    // no effect when set to 0
    uint64_t ttl{0};
    /// custom callback when conflict happens. Pass the error code and error message as parameter
    std::function<void(int, const std::string &)> callback;
};

using SinglePutRequest = SingleRequest;
using SingleDeleteRequest = SingleRequest;

struct BatchCommitRequest
{
    BatchCommitRequest(const bool with_cas_ = true, const bool allow_cas_fail_ = false)
        : with_cas(with_cas_), allow_cas_fail(allow_cas_fail_)
    {
    }

    void AddPut(const SinglePutRequest & put)
    {
        puts.emplace_back(put);
        request_size_in_bytes += put.size();
    }
    void AddDelete(const SingleDeleteRequest & del)
    {
        deletes.emplace_back(del);
        request_size_in_bytes += del.size();
    }
    void AddDelete(const std::string & del)
    {
        deletes.emplace_back(del);
        request_size_in_bytes += del.size();
    }
    void AddDelete(const std::string & delkey, const std::string & expected, const uint64_t & expected_version=0)
    {
        deletes.emplace_back(delkey, "", expected, expected_version);
        request_size_in_bytes += delkey.size() + expected.size();
    }
    void SetTimeout(uint32_t time_out) { commit_timeout_ms = time_out; }
    bool isEmpty() const { return puts.empty() && deletes.empty(); }

    uint32_t size() const { return request_size_in_bytes; }

    std::vector<SinglePutRequest> puts;
    std::vector<SingleDeleteRequest> deletes;
    bool with_cas = true;
    bool allow_cas_fail = true;
    uint32_t commit_timeout_ms = 0;
    uint32_t request_size_in_bytes = 0;
};

/// Response for batch commit request.It contains the conflict info for both put requests and delete requests.
/// The map represents the conflicting requests index in BatchCommit and its corresponding current value in metastore.
struct BatchCommitResponse
{
    std::unordered_map<int, std::string> puts;
    std::unordered_map<int, std::string> deletes;
    void reset() { puts.clear(); deletes.clear(); }
};

}

}
