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

#include <Parsers/IAST.h>
#include <QueryPlan/TableScanStep.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
/// this internal AST is only used by optimizer.
/// Represent the origin table and column of a identifier.
class ASTTableColumnReference : public IAST
{
public:
    const IStorage * storage;
    // the node id of the TableScanStep, used to identify different occurrence of a same table in self-join cases.
    size_t unique_id;
    String column_name;

    ASTTableColumnReference(const IStorage * storage_, size_t unique_id_, String column_name_)
        : storage(storage_), unique_id(unique_id_), column_name(std::move(column_name_))
    {
    }

    String getID(char delim) const override;

    void appendColumnName(WriteBuffer &) const override;

    ASTType getType() const override { return ASTType::ASTTableColumnReference; }

    ASTPtr clone() const override { return std::make_shared<ASTTableColumnReference>(storage, unique_id, column_name); }

    void toLowerCase() override { boost::to_lower(column_name); }

    void toUpperCase() override { boost::to_upper(column_name); }

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

struct TableInputRef
{
    StoragePtr storage;
    size_t unique_id;

    String getDatabaseTableName() const { return storage->getStorageID().getFullTableName(); }
    String toString() const { return getDatabaseTableName() + "#" + std::to_string(unique_id); }
};

struct TableInputRefHash
{
    size_t operator()(const TableInputRef & ref) const { return std::hash<UInt32>()(ref.unique_id); }
};

struct TableInputRefEqual
{
    bool operator()(const TableInputRef & lhs, const TableInputRef & rhs) const
    {
        return lhs.storage == rhs.storage && lhs.unique_id == rhs.unique_id;
    }
};

}
