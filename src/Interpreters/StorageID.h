/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once
#include <common/types.h>
#include <Common/DefaultCatalogName.h>
#include <Core/UUID.h>
#include <tuple>
#include <Core/QualifiedTableName.h>
#include <Common/DefaultCatalogName.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <common/types.h>
#include "Interpreters/Set.h"

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
}

static constexpr char const * TABLE_WITH_UUID_NAME_PLACEHOLDER = "_";
static constexpr char const * TABLE_PLACEHOLDER_FOR_ONLY_DATABASE = "_NOT_A_TABLE";

class ASTQueryWithTableAndOutput;
class ASTTableIdentifier;
class Context;
class WriteBuffer;
class ReadBuffer;
namespace Protos
{
    class StorageID;
}

// TODO(ilezhankin): refactor and merge |ASTTableIdentifier|
struct StorageID
{
    String database_name;
    String table_name;
    UUID uuid = UUIDHelpers::Nil;
    String server_vw_name = DEFAULT_SERVER_VW_NAME;

    StorageID(const String & database, const String & table, UUID uuid_ = UUIDHelpers::Nil)
        : database_name(database), table_name(table), uuid(uuid_)
    {
        assertNotEmpty();
    }

    explicit StorageID(const String & database, UUID uuid_ = UUIDHelpers::Nil)
        : database_name(database), table_name(TABLE_PLACEHOLDER_FOR_ONLY_DATABASE), uuid(uuid_)
    {
        assertNotEmpty();
    }

    StorageID(const QualifiedTableName & qualified_name);

    StorageID(const ASTQueryWithTableAndOutput & query);
    StorageID(const ASTTableIdentifier & table_identifier_node);
    StorageID(const ASTPtr & node);

    String getDatabaseName() const;

    String getTableName() const;

    String getFullTableName() const;
    String getFullNameNotQuoted() const;

    String getNameForLogs() const;
    String getDatabaseNameForLogs() const;

    explicit operator bool () const
    {
        return !empty();
    }

    bool empty() const
    {
        return table_name.empty() && !hasUUID();
    }

    bool hasUUID() const
    {
        return uuid != UUIDHelpers::Nil;
    }

    void clearUUID()
    { 
        uuid = UUIDHelpers::Nil;
    }

    bool isDatabase() const
    {
        return table_name == TABLE_PLACEHOLDER_FOR_ONLY_DATABASE;
    }

    bool operator<(const StorageID & rhs) const;
    bool operator==(const StorageID & rhs) const;

    void assertNotEmpty() const
    {
        // Can be triggered by user input, e.g. SELECT joinGetOrNull('', 'num', 500)
        if (empty())
            throw Exception("Both table name and UUID are empty", ErrorCodes::UNKNOWN_TABLE);
        if (table_name.empty() && !database_name.empty())
            throw Exception("Table name is empty, but database name is not", ErrorCodes::UNKNOWN_TABLE);
        if (table_name == TABLE_PLACEHOLDER_FOR_ONLY_DATABASE && (database_name.empty() || !hasUUID()))
            throw Exception("Database StorageID has no database name as well as UUID", ErrorCodes::LOGICAL_ERROR);
    }

    /// Avoid implicit construction of empty StorageID. However, it's needed for deferred initialization.
    static StorageID createEmpty() { return {}; }

    QualifiedTableName getQualifiedName() const { return {database_name, getTableName()}; }

    static StorageID fromDictionaryConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

    /// If dictionary has UUID, then use it as dictionary name in ExternalLoader to allow dictionary renaming.
    /// ExternalDictnariesLoader::resolveDictionaryName(...) should be used to access such dictionaries by name.
    String getInternalDictionaryName() const;

    void toProto(Protos::StorageID & proto) const;
    static StorageID fromProto(const Protos::StorageID & proto, ContextPtr context);
    static StorageID tryFromProto(const Protos::StorageID & proto, ContextPtr context);

    UInt64 hash() const
    {
        return getQualifiedName().hash();
    }

    /// Calculates hash using only the database and table name of a StorageID.
    struct DatabaseAndTableNameHash
    {
        size_t operator()(const StorageID & storage_id) const
        {
            SipHash hash_state;
            hash_state.update(storage_id.database_name.data(), storage_id.database_name.size());
            hash_state.update(storage_id.table_name.data(), storage_id.table_name.size());
            return hash_state.get64();
        }
    };

    /// Checks if the database and table name of two StorageIDs are equal.
    struct DatabaseAndTableNameEqual
    {
        bool operator()(const StorageID & left, const StorageID & right) const
        {
            return (left.database_name == right.database_name) && (left.table_name == right.table_name);
        }
    };

private:
    StorageID() = default;
};

}

namespace std
{
template <>
struct hash<DB::StorageID>
{
    using argument_type = DB::StorageID;
    using result_type = size_t;

    result_type operator()(const argument_type & storage_id) const
    {
        return storage_id.hash();
    }
};
}

namespace fmt
{
template <>
struct formatter<DB::StorageID>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::StorageID & storage_id, FormatContext & ctx)
    {
        return fmt::format_to(ctx.out(), "{}", storage_id.getNameForLogs());
    }
};
}
