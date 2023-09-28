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

#include <Common/Config/MetastoreConfig.h>
#include <Catalog/IMetastore.h>

namespace DB
{

namespace TSO
{

class TSOProxy
{
public:
    explicit TSOProxy(std::shared_ptr<Catalog::IMetaStore> metastore_ptr_, std::string key_)
        : metastore_ptr(std::move(metastore_ptr_))
        , key(std::move(key_))
    {}

    ~TSOProxy() = default;

    void setTimestamp(UInt64 timestamp);
    UInt64 getTimestamp();
    void clean();

private:
    std::shared_ptr<Catalog::IMetaStore> metastore_ptr;
    std::string key;
};

}

}
