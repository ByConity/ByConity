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

#include <Core/Field.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

/// used in worker RPC, don't break backward compatibility
enum class WorkerEngineType : uint8_t
{
    CLOUD = 0,  // CloudMergeTree
    DICT = 1,   // DictCloudMergeTree (for BitEngine dict table)
};

inline static String toString(WorkerEngineType type)
{
    switch (type)
    {
        case WorkerEngineType::CLOUD:
            return "Cloud";
        case WorkerEngineType::DICT:
            return "DictCloud";
    }
}

class ASTCreateQuery;
class ASTSetQuery;
class ASTStorage;

/// see Databases/DatabaseOnDisk.h
extern String getObjectDefinitionFromCreateQuery(const ASTPtr & query, std::optional<bool> attach);

std::shared_ptr<ASTCreateQuery> getASTCreateQueryFromString(const String & query, const ContextPtr & context);
std::shared_ptr<ASTCreateQuery> getASTCreateQueryFromStorage(const IStorage & storage, const ContextPtr & context);

StoragePtr createStorageFromQuery(ASTCreateQuery & create_query, ContextMutablePtr context);
StoragePtr createStorageFromQuery(const String & query, const ContextPtr & context);

/// change storage engine from Cnch-family to Cloud-family
/// TODO: can we get rid of engine_args?
void replaceCnchWithCloud(
    ASTStorage * storage,
    const String & cnch_database,
    const String & cnch_table,
    WorkerEngineType engine_type = WorkerEngineType::CLOUD,
    const Strings & engine_args = {});

void modifyOrAddSetting(ASTSetQuery & set_query, const String & name, Field value);
void modifyOrAddSetting(ASTCreateQuery & create_query, const String & name, Field value);

}
