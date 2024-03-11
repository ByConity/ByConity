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

#include <Storages/IStorage.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class Context;

class StorageSystemCnchPartsInfoLocal final : public shared_ptr_helper<StorageSystemCnchPartsInfoLocal>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemCnchPartsInfoLocal>;

public:
    std::string getName() const override { return "StorageSystemCnchPartsInfoLocal"; }

    enum ReadyState
    {
        Unloaded = 1,
        Loading = 2,
        Loaded = 3,
        Recalculated = 4,
    };

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool isSystemStorage() const override { return true; }

protected:
    explicit StorageSystemCnchPartsInfoLocal(const StorageID & table_id_);
};

}
