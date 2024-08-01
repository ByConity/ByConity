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

#include <optional>
#include <Core/NamesAndTypes.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MutationCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Common/SettingsChanges.h>


namespace DB
{

class ASTAlterCommand;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

/// Operation from the ALTER query (except for manipulation with PART/PARTITION).
/// Adding Nested columns is not expanded to add individual columns.
struct AlterCommand
{
    /// The AST of the whole command
    ASTPtr ast;

    enum Type
    {
        UNKNOWN,
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        COMMENT_COLUMN,
        MODIFY_ORDER_BY,
        MODIFY_CLUSTER_BY,
        DROP_CLUSTER,
        MODIFY_SAMPLE_BY,
        ADD_INDEX,
        DROP_INDEX,
        ADD_CONSTRAINT,
        DROP_CONSTRAINT,
        ADD_FOREIGN_KEY,
        DROP_FOREIGN_KEY,
        ADD_UNIQUE_NOT_ENFORCED,
        DROP_UNIQUE_NOT_ENFORCED,
        ADD_PROJECTION,
        DROP_PROJECTION,
        MODIFY_TTL,
        MODIFY_SETTING,
        RESET_SETTING,
        MODIFY_QUERY,
        RENAME_COLUMN,
        REMOVE_TTL,
        CLEAR_MAP_KEY,
        MATERIALIZE_PROJECTION,
        CHANGE_ENGINE,
        MODIFY_DATABASE_SETTING,
        RENAME_TABLE,
        DROP_PARTITION
    };

    /// Which property user wants to remove from column
    enum class RemoveProperty
    {
        NO_PROPERTY,
        /// Default specifiers
        DEFAULT,
        MATERIALIZED,
        ALIAS,

        /// Other properties
        COMMENT,
        CODEC,
        TTL,
    };

    Type type = UNKNOWN;

    String column_name;

    /// For DROP/CLEAR COLUMN/INDEX ... IN PARTITION
    ASTPtr partition;

    /// For CLEAR COLUMN IN PARTITION WHERE
    ASTPtr partition_predicate;

    /// For ADD and MODIFY, a new column type.
    DataTypePtr data_type = nullptr;

    ColumnDefaultKind default_kind{};
    ASTPtr default_expression{};

    /// For COMMENT column
    std::optional<String> comment;

    /// For ADD or MODIFY - after which column to add a new one. If an empty string, add to the end.
    String after_column;

    /// For ADD_COLUMN, MODIFY_COLUMN, ADD_INDEX - Add to the begin if it is true.
    bool first = false;

    /// For DROP_COLUMN, MODIFY_COLUMN, COMMENT_COLUMN, RESET_SETTING
    bool if_exists = false;

    /// For ADD_COLUMN
    bool if_not_exists = false;

    bool mysql_primary_key = false;

    /// For MODIFY_ORDER_BY
    ASTPtr order_by = nullptr;

    /// For MODIFY_CLUSTER_BY
    ASTPtr cluster_by = nullptr;

    /// For MODIFY_SAMPLE_BY
    ASTPtr sample_by = nullptr;

    /// For ADD INDEX
    ASTPtr index_decl = nullptr;
    String after_index_name;

    /// For ADD/DROP INDEX
    String index_name;

    // For ADD CONSTRAINT
    ASTPtr constraint_decl = nullptr;

    // For ADD/DROP CONSTRAINT
    String constraint_name;

    // For ADD FOREIGN KEY
    ASTPtr foreign_key_decl = nullptr;

    // FOR ADD/DROP FOREIGN_KEY
    String foreign_key_name;

    // For ADD UNIQUE NOT ENFORCED
    ASTPtr unique_not_enforced_decl = nullptr;

    // FOR ADD/DROP UNIQUE NOT ENFORCED
    String unique_not_enforced_name;

    /// For ADD PROJECTION
    ASTPtr projection_decl = nullptr;
    String after_projection_name;

    /// For ADD/DROP PROJECTION
    String projection_name;

    /// For MODIFY TTL
    ASTPtr ttl = nullptr;

    /// indicates that this command should not be applied, for example in case of if_exists=true and column doesn't exist.
    bool ignore = false;

    /// Clear columns or index (don't drop from metadata)
    bool clear = false;

    /// For ADD and MODIFY
    ASTPtr codec = nullptr;

    /// For MODIFY SETTING
    SettingsChanges settings_changes;

    /// For RESET SETTING
    std::set<String> settings_resets;

    /// For MODIFY_QUERY
    ASTPtr select = nullptr;

    /// Target column name
    String rename_to;

    /// For CLEAR MAP KEYS
    ASTPtr map_keys;

    /// For change engine
    ASTPtr engine;

    /// What to remove from column (or TTL)
    RemoveProperty to_remove = RemoveProperty::NO_PROPERTY;

    static std::optional<AlterCommand> parse(const ASTAlterCommand * command, ContextPtr context);

    void apply(StorageInMemoryMetadata & metadata, ContextPtr context, bool allow_nullable_key = false) const;

    /// Check that alter command require data modification (mutation) to be
    /// executed. For example, cast from Date to UInt16 type can be executed
    /// without any data modifications. But column drop or modify from UInt16 to
    /// UInt32 require data modification.
    bool isRequireMutationStage(const StorageInMemoryMetadata & metadata) const;

    /// Checks that only settings changed by alter
    bool isSettingsAlter() const;

    /// Checks that only comment changed by alter
    bool isCommentAlter() const;

    /// Checks that any TTL changed by alter
    bool isTTLAlter(const StorageInMemoryMetadata & metadata) const;

    /// Command removing some property from column or table
    bool isRemovingProperty() const;

    /// If possible, convert alter command to mutation command. In other case
    /// return empty optional. Some storages may execute mutations after
    /// metadata changes.
    std::optional<MutationCommand> tryConvertToMutationCommand(StorageInMemoryMetadata & metadata, ContextPtr context) const;
};

/// Return string representation of AlterCommand::Type
String alterTypeToString(const AlterCommand::Type type);

class Context;

/// Vector of AlterCommand with several additional functions
class AlterCommands : public std::vector<AlterCommand>
{
private:
    bool prepared = false;

public:
    /// Validate that commands can be applied to metadata.
    /// Checks that all columns exist and dependencies between them.
    /// This check is lightweight and base only on metadata.
    /// More accurate check have to be performed with storage->checkAlterIsPossible.
    void validate(const StorageInMemoryMetadata & metadata, ContextPtr context) const;

    /// Prepare alter commands. Set ignore flag to some of them and set some
    /// parts to commands from storage's metadata (for example, absent default)
    void prepare(const StorageInMemoryMetadata & metadata);

    /// Apply all alter command in sequential order to storage metadata.
    /// Commands have to be prepared before apply.
    void apply(StorageInMemoryMetadata & metadata, ContextPtr context) const;

    /// At least one command modify settings.
    bool hasSettingsAlterCommand() const;

    /// All commands modify settings only.
    bool isSettingsAlter() const;

    /// All commands modify comments only.
    bool isCommentAlter() const;

    /// Return mutation commands which some storages may execute as part of
    /// alter. If alter can be performed as pure metadata update, than result is
    /// empty. If some TTL changes happened than, depending on materialize_ttl
    /// additional mutation command (MATERIALIZE_TTL) will be returned.
    MutationCommands getMutationCommands(StorageInMemoryMetadata metadata, bool materialize_ttl, ContextPtr context) const;
};

}
