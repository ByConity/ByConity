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

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/IAST.h>
#include "Parsers/IAST_fwd.h"


namespace DB
{

/** ALTER query:
 *  ALTER TABLE [db.]name_type
 *      ADD COLUMN col_name type [AFTER col_after],
 *      DROP COLUMN col_drop [FROM PARTITION partition],
 *      MODIFY COLUMN col_name type,
 *      DROP PARTITION partition,
 *      COMMENT_COLUMN col_name 'comment',
 *  ALTER LIVE VIEW [db.]name_type
 *      REFRESH
 */



/// JUST APPEND after the tail of enum Type, or
/// the CI test 01604_explain_ast_of_nonselect_query may fail
class ASTAlterCommand : public IAST
{
public:
    enum Type
    {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        COMMENT_COLUMN,
        RENAME_COLUMN,
        MODIFY_ORDER_BY,
        MODIFY_SAMPLE_BY,
        MODIFY_TTL,
        MATERIALIZE_TTL,
        MODIFY_SETTING,
        RESET_SETTING,
        MODIFY_QUERY,
        REMOVE_TTL,
        MODIFY_CLUSTER_BY,
        DROP_CLUSTER,

        ADD_INDEX,
        DROP_INDEX,
        MATERIALIZE_INDEX,

        ADD_CONSTRAINT,
        DROP_CONSTRAINT,

        ADD_FOREIGN_KEY,
        DROP_FOREIGN_KEY,

        ADD_UNIQUE_NOT_ENFORCED,
        DROP_UNIQUE_NOT_ENFORCED,

        ADD_PROJECTION,
        DROP_PROJECTION,
        MATERIALIZE_PROJECTION,

        DROP_PARTITION,
        DROP_DETACHED_PARTITION,
        ATTACH_PARTITION,
        ATTACH_DETACHED_PARTITION,
        MOVE_PARTITION,
        MOVE_PARTITION_FROM,
        REPLACE_PARTITION,
        REPLACE_PARTITION_WHERE,
        INGEST_PARTITION,
        FETCH_PARTITION,
        FREEZE_PARTITION,
        FREEZE_ALL,
        UNFREEZE_PARTITION,
        UNFREEZE_ALL,
        PREATTACH_PARTITION,

        DROP_PARTITION_WHERE,
        FETCH_PARTITION_WHERE,
        RECLUSTER_PARTITION_WHERE,

        BUILD_BITMAP_OF_PARTITION_WHERE,
        BUILD_BITMAP_OF_PARTITION,
        DROP_BITMAP_OF_PARTITION_WHERE,
        DROP_BITMAP_OF_PARTITION,
        BUILD_MARK_BITMAP_OF_PARTITION_WHERE,
        BUILD_MARK_BITMAP_OF_PARTITION,
        DROP_MARK_BITMAP_OF_PARTITION_WHERE,
        DROP_MARK_BITMAP_OF_PARTITION,
        REPAIR_PARTITION,

        DELETE,
        FAST_DELETE,
        UPDATE,

        CLEAR_MAP_KEY,

        NO_TYPE,

        LIVE_VIEW_REFRESH,

        SAMPLE_PARTITION_WHERE,

        CHANGE_ENGINE,

        MODIFY_DATABASE_SETTING,

        RENAME_TABLE,

        PARTITION_BY,
    };

    Type type = NO_TYPE;

    /** The ADD COLUMN query stores the name and type of the column to add
     *  This field is not used in the DROP query
     *  In MODIFY query, the column name and the new type are stored here
     */
    ASTPtr col_decl;

    /** The ADD COLUMN and MODIFY COLUMN query here optionally stores the name of the column following AFTER
     * The DROP query stores the column name for deletion here
     * Also used for RENAME COLUMN.
     */
    ASTPtr column;

    /** For MODIFY ORDER BY
     */
    ASTPtr order_by;

    /** For MODIFY CLUSTER BY
     */
    ASTPtr cluster_by;


    /** For MODIFY SAMPLE BY
     */
    ASTPtr sample_by;

    /** The ADD INDEX query stores the IndexDeclaration there.
     */
    ASTPtr index_decl;

    /** The ADD INDEX query stores the name of the index following AFTER.
     *  The DROP INDEX query stores the name for deletion.
     *  The MATERIALIZE INDEX query stores the name of the index to materialize.
     *  The CLEAR INDEX query stores the name of the index to clear.
     */
    ASTPtr index;

    /** The ADD CONSTRAINT query stores the ConstraintDeclaration there.
    */
    ASTPtr constraint_decl;

    /** The ADD FOREIGN KEY query stores the ForeignKeyDeclaration there.
    */
    ASTPtr foreign_key_decl;

    /** The ADD UNIQUE query stores the ForeignKeyDeclaration there.
    */
    ASTPtr unique_not_enforced_decl;

    /** The DROP CONSTRAINT query stores the name for deletion.
    */
    ASTPtr constraint;

    /** The DROP FOREIGN KEY query stores the name for deletion.
    */
    ASTPtr foreign_key;

    /** The DROP UNIQUE query stores the name for deletion.
    */
    ASTPtr unique_not_enforced;

    /** The ADD PROJECTION query stores the ProjectionDeclaration there.
     */
    ASTPtr projection_decl;

    /** The ADD PROJECTION query stores the name of the projection following AFTER.
     *  The DROP PROJECTION query stores the name for deletion.
     *  The MATERIALIZE PROJECTION query stores the name of the projection to materialize.
     *  The CLEAR PROJECTION query stores the name of the projection to clear.
     */
    ASTPtr projection;

    /** Used in DROP PARTITION, ATTACH PARTITION FROM, UPDATE, DELETE, FASTDELETE queries.
     *  The value or ID of the partition is stored here.
     */
    ASTPtr partition;

    /// For DELETE/FASTDELETE/UPDATE WHERE: the predicate that filters the rows to delete/update.
    ASTPtr predicate;

    /// A list of expressions of the form `column = expr` for the UPDATE command.
    ASTPtr update_assignments;

    /// A column comment
    ASTPtr comment;

    /// For MODIFY TTL query
    ASTPtr ttl;

    /// FOR MODIFY_SETTING
    ASTPtr settings_changes;

    /// FOR RESET_SETTING
    ASTPtr settings_resets;

    /// For MODIFY_QUERY
    ASTPtr select;

    /** In ALTER CHANNEL, ADD, DROP, SUSPEND, RESUME, REFRESH, MODIFY queries, the list of live views is stored here
     */
    ASTPtr values;

    /// For CLEAR MAP KEY map_column('map_key1', 'map_key2'...)
    ASTPtr map_keys;

    /// For INGESTION query, the optional list of columns to overwrite
    ASTPtr columns;
    /// For Ingestion columns
    ASTPtr keys;
    /// The INGEST PARTITION query here optionally stores the buckets that going to be ingested
    ASTPtr buckets;

    /// For sample / split / resharding expression
    ASTPtr with_sharding_exp;

    /// For Engine = EngineName(...), the name with optional parameters is stored here.
    ASTPtr engine;

    bool detach = false;        /// true for DETACH PARTITION

    bool attach_from_detached = false;  /// true for ATTACH DETACHED PARTITION.

    bool part = false;          /// true for ATTACH PART, DROP DETACHED PART, REPAIR PART and MOVE

    bool parts = false;         /// true for ATTACH PARTS from directory

    bool clear_column = false;  /// for CLEAR COLUMN (do not drop column from metadata)

    bool clear_index = false;   /// for CLEAR INDEX (do not drop index from metadata)

    bool clear_projection = false;   /// for CLEAR PROJECTION (do not drop projection from metadata)

    bool if_not_exists = false; /// option for ADD_COLUMN

    bool if_exists = false;     /// option for DROP_COLUMN, MODIFY_COLUMN, COMMENT_COLUMN

    bool first = false;         /// option for ADD_COLUMN, MODIFY_COLUMN

    bool cascading = false; /// true for DROP/DETACH PARTITION [WHERE]

    bool staging_area = false;  /// true for DETACH/DROP STAGED PARTITION/PART, only used by unique table

    DataDestinationType move_destination_type; /// option for MOVE PART/PARTITION

    String move_destination_name;             /// option for MOVE PART/PARTITION

    /** For FETCH PARTITION - the path in ZK to the shard, from which to download the partition.
     */
    String from;

    /**
     * For FREEZE PARTITION - place local backup to directory with specified name.
     * For UNFREEZE - delete local backup at directory with specified name.
     */
    String with_name;

    /// REPLACE(ATTACH) PARTITION partition FROM db.table
    String from_database;
    String from_table;
    /// To distinguish REPLACE and ATTACH PARTITION partition FROM db.table
    bool replace = true;
    /// MOVE PARTITION partition TO TABLE db.table
    String to_database;
    String to_table;

    /// Target column namea
    ASTPtr rename_to;

    ASTPtr rename_table_to;
    ASTPtr partition_by;
    ASTPtr storage_policy;
    ASTPtr hot_partition_count;
    ASTPtr rt_engine;
    ASTPtr life_cycle;
    bool all = false;

    /// Which property user want to remove
    String remove_property;

    /// For DETACH/ATTACH PARTITION commands
    bool specify_bucket = false;
    UInt64 bucket_number;

    String getID(char delim) const override { return "AlterCommand" + (delim + std::to_string(static_cast<int>(type))); }

    ASTPtr clone() const override;

    ASTType getType() const override { return ASTType::ASTAlterCommand; }

    void toLowerCase() override
    {
        boost::to_lower(from_database);
        boost::to_lower(from_table);
        boost::to_lower(to_database);
        boost::to_lower(to_table);
    }

    void toUpperCase() override
    {
        boost::to_upper(from_database);
        boost::to_upper(from_table);
        boost::to_upper(to_database);
        boost::to_upper(to_table);
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

class ASTAlterQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    enum class AlterObjectType
    {
        TABLE,
        DATABASE,
        LIVE_VIEW,
        UNKNOWN,
    };

    AlterObjectType alter_object = AlterObjectType::UNKNOWN;

    ASTExpressionList * command_list = nullptr;

    bool isSettingsAlter() const;

    bool isFreezeAlter() const;

    String getID(char) const override;

    ASTType getType() const override { return ASTType::ASTAlterQuery; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeOnCluster<ASTAlterQuery>(clone(), new_database);
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    bool isOneCommandTypeOnly(const ASTAlterCommand::Type & type) const;
};

class ASTAlterAnalyticalMySQLQuery : public ASTAlterQuery
{
};

}
