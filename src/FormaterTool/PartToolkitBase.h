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
#include <unordered_map>
#include <Core/Field.h>
#include <IO/S3Common.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageMergeTree.h>

namespace DB
{

#define PT_RELATIVE_LOCAL_PATH "data/"

// Only support S3.
class S3CleanTaskInfo
{
public:
    enum CleanType
    {
        META = 0,
        DATA = 1,
        ALL = 2,
    };

    CleanType type;
    String task_id;
};

class PartToolkitBase : public WithMutableContext
{
public:
    PartToolkitBase(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    virtual void execute() = 0;

    virtual ~PartToolkitBase();

protected:
    void applySettings();

    StoragePtr getTable();

    void processIgnoreBitEngineEncode(ColumnsDescription & columns);

    PartNamesWithDisks collectPartsFromSource(const String & source_dirs_str, const String & dest_dir);

    const ASTPtr & query_ptr;
    std::unordered_map<String, Field> user_settings;

    /// Only used in S3 mode.
    /// S3 config file for input.
    std::unique_ptr<S3::S3Config> s3_input_config;
    /// S3 config file for output.
    std::unique_ptr<S3::S3Config> s3_output_config;

private:
    StoragePtr storage = nullptr;
};

}
