/*
 * Copyright (2023) Bytedance Ltd. and/or its affiliates
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

#include <memory>
#include <common/types.h>
#include <Common/Config/MetastoreConfig.h>
#include <Catalog/IMetastore.h>
#include <Catalog/MetastoreFDBImpl.h>
#include <Catalog/MetastoreByteKVImpl.h>

namespace DB
{

class IKvStorage
{
public:
    virtual ~IKvStorage() = default;

    /**
     * Save a record into metastore;
     */
    virtual void put(const String & key, const String & value, bool if_not_exists = false) = 0;

    /**
     * Put with CAS. Return true if CAS succeed, otherwise return false with current value.
     */
    virtual std::pair<bool, String> putCAS(const String & key, const String & value, const String & expected, bool with_old_value = false) = 0;

    /**
     * Get a record by name from metastore;
     */
    virtual uint64_t get(const String & key, String & value) = 0;
};

using IKvStoragePtr = std::shared_ptr<IKvStorage>;

class TSOKvStorage : public IKvStorage
{
public:
    explicit TSOKvStorage(std::shared_ptr<Catalog::IMetaStore> store_) : metastore_ptr(std::move(store_)) {}

    void put(const String & key, const String & value, bool if_not_exists = false) override
    {
        metastore_ptr->put(key, value, if_not_exists);
    }

    std::pair<bool, String> putCAS(const String & key, const String & value, const String & expected, bool with_old_value = false) override
    {
        return metastore_ptr->putCAS(key, value, expected, with_old_value);
    }

    uint64_t get(const String & key, String & value) override
    {
        return metastore_ptr->get(key, value);
    }

private:
    std::shared_ptr<Catalog::IMetaStore> metastore_ptr;
};

class ResourceManagerKvStorage : public IKvStorage
{
    std::shared_ptr<Catalog::IMetaStore> store;

public:
    explicit ResourceManagerKvStorage(std::shared_ptr<Catalog::IMetaStore> store_) : store(std::move(store_)) { }

    void put(const String & key, const String & value, bool if_not_exists = false) override
    {
        store->put(key, value, if_not_exists);
    }

    std::pair<bool, String> putCAS(const String & key, const String & value, const String & expected, bool with_old_value = false) override
    {
        return store->putCAS(key, value, expected, with_old_value);
    }

    uint64_t get(const String & key, String & value) override
    {
        return store->get(key, value);
    }
};

class ServerManagerKvStorage : public IKvStorage
{
    std::shared_ptr<Catalog::IMetaStore> store;

public:
    explicit ServerManagerKvStorage(std::shared_ptr<Catalog::IMetaStore> store_) : store(std::move(store_)) { }

    void put(const String & key, const String & value, bool if_not_exists = false) override
    {
        store->put(key, value, if_not_exists);
    }

    std::pair<bool, String> putCAS(const String & key, const String & value, const String & expected, bool with_old_value = false) override
    {
        return store->putCAS(key, value, expected, with_old_value);
    }

    uint64_t get(const String & key, String & value) override
    {
        return store->get(key, value);
    }
};

}
