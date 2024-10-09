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

#include <MergeTreeCommon/MergeTreeMetaBase.h>

#include <Catalog/Catalog.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/RowExistsColumnInfo.h>
#include <Common/SipHash.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/SettingsEnums.h>
#include <Storages/DataDestinationType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/MapHelpers.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/ObjectUtils.h>
#include <IO/ConcatReadBuffer.h>
#include <Parsers/ASTClusterByElement.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTUtils.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageDictCloudMergeTree.h>
#include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MergeTree/localBackup.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/MutationCommands.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/CnchSystemLog.h>
#include <Functions/IFunction.h>
#include <QueryPlan/QueryIdHolder.h>
#include <Parsers/queryToString.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/PartitionPredicateVisitor.h>
#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/EqualityASTMap.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SelectQueryInfoHelper.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/PartCacheManager.h>
#include <MergeTreeCommon/IMergeTreePartMeta.h>
#include <CloudServices/CnchPartsHelper.h>

#include <common/scope_guard_safe.h>


namespace ProfileEvents
{
    extern const Event CatalogTime;
    extern const Event RejectedInserts;
}

namespace
{
    constexpr UInt64 RESERVATION_MIN_ESTIMATION_SIZE = 1u * 1024u * 1024u; /// 1MB
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int INVALID_PARTITION_VALUE;
    extern const int UNKNOWN_PART_TYPE;
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int TOO_MANY_PARTS;
    extern const int NOT_ENOUGH_SPACE;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_TTL_EXPRESSION;
    extern const int CANNOT_PARSE_TEXT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNSUPPORTED_PARAMETER;
}

void MergeTreeMetaBase::checkSampleExpression(const StorageInMemoryMetadata & metadata, bool allow_sampling_expression_not_in_primary_key)
{
    const auto & pk_sample_block = metadata.getPrimaryKey().sample_block;
    if (!pk_sample_block.has(metadata.sampling_key.column_names[0]) && !allow_sampling_expression_not_in_primary_key)
        throw Exception("Sampling expression must be present in the primary key", ErrorCodes::BAD_ARGUMENTS);
}

MergeTreeMetaBase::MergeTreeMetaBase(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    const String & logger_name_,
    bool require_part_metadata_,
    bool attach_,
    BrokenPartCallback broken_part_callback_)
    : IStorage(table_id_)
    , WithMutableContext(context_->getGlobalContext())
    , unique_key_index_cache(getContext()->getUniqueKeyIndexCache())
    , merging_params(merging_params_)
    , require_part_metadata(require_part_metadata_)
    , broken_part_callback(broken_part_callback_)
    , log_name(logger_name_)
    , log(::getLogger(log_name))
    , storage_settings(std::move(storage_settings_))
    , pinned_part_uuids(std::make_shared<PinnedPartUUIDs>())
    , data_parts_by_info(data_parts_indexes.get<TagByInfo>())
    , data_parts_by_state_and_info(data_parts_indexes.get<TagByStateAndInfo>())
    , relative_data_path(relative_data_path_)
{
    const auto & settings = getSettings();
    allow_nullable_key = attach_ || settings->allow_nullable_key || settings->enable_nullable_sorting_key;
    if (!date_column_name.empty())
    {
        try
        {
            checkPartitionKeyAndInitMinMax(metadata_.partition_key);
            if (minmax_idx_date_column_pos == -1)
                throw Exception("Could not find Date column", ErrorCodes::BAD_TYPE_OF_FIELD);
        }
        catch (Exception & e)
        {
            /// Better error message.
            e.addMessage("(while initializing MergeTree partition key from date column " + backQuote(date_column_name) + ")");
            throw;
        }
    }
    else
    {
        is_custom_partitioned = true;
        checkPartitionKeyAndInitMinMax(metadata_.partition_key);
    }

    storage_address = fmt::format("{}", fmt::ptr(this));

    /// NOTE: using the same columns list as is read when performing actual merges.
    merging_params.check(metadata_, attach_);

    if (metadata_.sampling_key.definition_ast != nullptr)
    {
        /// This is for backward compatibility.
        checkSampleExpression(metadata_, getSettings()->compatibility_allow_sampling_expression_not_in_primary_key);
    }
}

StoragePolicyPtr MergeTreeMetaBase::getStoragePolicy(StorageLocation location) const
{
    if (unlikely(location == StorageLocation::AUXILITY))
    {
        throw Exception("Get auxility storage policy is not supported",
            ErrorCodes::LOGICAL_ERROR);
    }

    /// This logic is only for StorageMergeTree now, thus we only need to check cloudfs
    // if (getSettings()->enable_cloudfs || getContext()->getSettingsRef().enable_cloudfs)
    // {
    //     const String & storage_policy_name = getSettings()->storage_policy.value + CLOUDFS_STORAGE_POLICY_SUFFIX;
    //     auto policy = getContext()->tryGetStoragePolicy(storage_policy_name);
    //     if (policy)
    //         return policy;
    //     else
    //         LOG_WARNING(log, "Storage Policy {} is not found and will fallback to use ufs storage policy", storage_policy_name);
    // }

    return getContext()->getStoragePolicy(getSettings()->storage_policy);
}

const String& MergeTreeMetaBase::getRelativeDataPath(StorageLocation location) const
{
    if (unlikely(location == StorageLocation::AUXILITY))
    {
        throw Exception("Get auxility relative data path is not supported",
            ErrorCodes::LOGICAL_ERROR);
    }
    return relative_data_path;
}

void MergeTreeMetaBase::setRelativeDataPath(StorageLocation location, const String & rel_path)
{
    if (unlikely(location == StorageLocation::AUXILITY))
    {
        throw Exception("Set auxility relative data path is not supported",
            ErrorCodes::LOGICAL_ERROR);
    }
    relative_data_path = rel_path;
}

static void checkKeyExpression(const ExpressionActions & expr, const Block & sample_block, const String & key_name, bool allow_nullable_key)
{
    for (const auto & action : expr.getActions())
    {
        if (action.node->type == ActionsDAG::ActionType::ARRAY_JOIN)
            throw Exception(key_name + " key cannot contain array joins", ErrorCodes::ILLEGAL_COLUMN);

        if (action.node->type == ActionsDAG::ActionType::FUNCTION)
        {
            IFunctionBase & func = *action.node->function_base;
            if (!func.isDeterministic())
                throw Exception(key_name + " key cannot contain non-deterministic functions, "
                    "but contains function " + func.getName(),
                    ErrorCodes::BAD_ARGUMENTS);
        }
    }

    for (const ColumnWithTypeAndName & element : sample_block)
    {
        const ColumnPtr & column = element.column;
        if (column && (isColumnConst(*column) || column->isDummy()))
            throw Exception{key_name + " key cannot contain constants", ErrorCodes::ILLEGAL_COLUMN};

        if (!allow_nullable_key && element.type->isNullable())
            throw Exception{key_name + " key cannot contain nullable columns", ErrorCodes::ILLEGAL_COLUMN};
    }
}

void MergeTreeMetaBase::checkProperties(
    const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata, bool attach) const
{
    if (!new_metadata.sorting_key.definition_ast)
        throw Exception("ORDER BY cannot be empty", ErrorCodes::BAD_ARGUMENTS);

    KeyDescription new_sorting_key = new_metadata.sorting_key;
    KeyDescription new_primary_key = new_metadata.primary_key;
    KeyDescription new_unique_key = new_metadata.unique_key;

    size_t sorting_key_size = new_sorting_key.column_names.size();
    size_t primary_key_size = new_primary_key.column_names.size();
    if (primary_key_size > sorting_key_size)
        throw Exception("Primary key must be a prefix of the sorting key, but its length: "
            + toString(primary_key_size) + " is greater than the sorting key length: " + toString(sorting_key_size),
            ErrorCodes::BAD_ARGUMENTS);

    NameSet primary_key_columns_set;

    for (size_t i = 0; i < sorting_key_size; ++i)
    {
        const String & sorting_key_column = new_sorting_key.column_names[i];

        if (i < primary_key_size)
        {
            const String & pk_column = new_primary_key.column_names[i];
            if (pk_column != sorting_key_column)
                throw Exception("Primary key must be a prefix of the sorting key, but the column in the position "
                    + toString(i) + " is " + sorting_key_column +", not " + pk_column,
                    ErrorCodes::BAD_ARGUMENTS);

            // for compatibility with cnch-1.4, which allows dup key in `order by`
 //           if (!primary_key_columns_set.emplace(pk_column).second)
 //               throw Exception("Primary key contains duplicate columns", ErrorCodes::BAD_ARGUMENTS);

        }
    }

    auto all_columns = new_metadata.columns.getAllPhysical();

    /// Order by check AST
    if (old_metadata.hasSortingKey())
    {
        /// This is ALTER, not CREATE/ATTACH TABLE. Let us check that all new columns used in the sorting key
        /// expression have just been added (so that the sorting order is guaranteed to be valid with the new key).

        Names new_primary_key_columns = new_primary_key.column_names;
        Names new_sorting_key_columns = new_sorting_key.column_names;

        ASTPtr added_key_column_expr_list = std::make_shared<ASTExpressionList>();
        const auto & old_sorting_key_columns = old_metadata.getSortingKeyColumns();
        for (size_t new_i = 0, old_i = 0; new_i < sorting_key_size; ++new_i)
        {
            if (old_i < old_sorting_key_columns.size())
            {
                if (new_sorting_key_columns[new_i] != old_sorting_key_columns[old_i])
                    added_key_column_expr_list->children.push_back(new_sorting_key.expression_list_ast->children[new_i]);
                else
                    ++old_i;
            }
            else
                added_key_column_expr_list->children.push_back(new_sorting_key.expression_list_ast->children[new_i]);
        }

        if (!added_key_column_expr_list->children.empty())
        {
            auto syntax = TreeRewriter(getContext()).analyze(added_key_column_expr_list, all_columns);
            Names used_columns = syntax->requiredSourceColumns();

            NamesAndTypesList deleted_columns;
            NamesAndTypesList added_columns;
            old_metadata.getColumns().getAllPhysical().getDifference(all_columns, deleted_columns, added_columns);

            for (const String & col : used_columns)
            {
                if (!added_columns.contains(col) || deleted_columns.contains(col))
                    throw Exception("Existing column " + backQuoteIfNeed(col) + " is used in the expression that was "
                        "added to the sorting key. You can add expressions that use only the newly added columns",
                        ErrorCodes::BAD_ARGUMENTS);

                if (new_metadata.columns.getDefaults().count(col))
                    throw Exception("Newly added column " + backQuoteIfNeed(col) + " has a default expression, so adding "
                        "expressions that use it to the sorting key is forbidden",
                        ErrorCodes::BAD_ARGUMENTS);
            }
        }
    }

    if (old_metadata.hasUniqueKey())
    {
        ASTPtr new_unique_key_expr_list = std::make_shared<ASTExpressionList>();
        for (size_t i = 0; i < new_unique_key.column_names.size(); ++i)
            new_unique_key_expr_list->children.push_back(new_unique_key.expression_list_ast->children[i]);

        auto new_unique_key_syntax = TreeRewriter(getContext()).analyze(new_unique_key_expr_list, all_columns);
        auto new_unique_key_expr = ExpressionAnalyzer(new_unique_key_expr_list, new_unique_key_syntax, getContext())
            .getActions(/*add_aliases*/false);
        auto new_unique_key_sample = ExpressionAnalyzer(new_unique_key_expr_list, new_unique_key_syntax, getContext())
            .getActions(/*add_aliases*/true)->getSampleBlock();

        checkKeyExpression(*new_unique_key_expr, new_unique_key_sample, "Unique", allow_nullable_key);

        /// check column type
        for (auto & col_with_type: new_unique_key_sample.getNamesAndTypesList())
        {
            auto serial = col_with_type.type->getDefaultSerialization();
            if (!serial->supportMemComparableEncoding())
                throw Exception("Column " + col_with_type.name + " can't be used in UNIQUE KEY because its type "
                                + col_with_type.type->getName() + " is not mem-comparable", ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    if (!new_metadata.secondary_indices.empty())
    {
        std::unordered_set<String> indices_names;

        for (const auto & index : new_metadata.secondary_indices)
        {

            MergeTreeIndexFactory::instance().validate(index, attach);

            if (indices_names.find(index.name) != indices_names.end())
                throw Exception(
                        "Index with name " + backQuote(index.name) + " already exists",
                        ErrorCodes::LOGICAL_ERROR);

            indices_names.insert(index.name);
        }
    }

    if (!new_metadata.projections.empty())
    {
        std::unordered_set<String> projections_names;

        for (const auto & projection : new_metadata.projections)
        {
            MergeTreeProjectionFactory::instance().validate(projection);

            if (projections_names.find(projection.name) != projections_names.end())
                throw Exception(
                        "Projection with name " + backQuote(projection.name) + " already exists",
                        ErrorCodes::LOGICAL_ERROR);

            projections_names.insert(projection.name);
        }
    }

    for (const auto & column : new_metadata.columns)
    {
        if (column.replace_if_not_null && !column.type->isNullable())
            throw Exception("REPLACE_IF_NOT_NULL could not used with nullable type, column name: " + column.name, ErrorCodes::LOGICAL_ERROR);
    }

    checkKeyExpression(*new_sorting_key.expression, new_sorting_key.sample_block, "Sorting", allow_nullable_key);

}

void MergeTreeMetaBase::setProperties(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata, bool attach)
{
    checkProperties(new_metadata, old_metadata, attach);
    setInMemoryMetadata(new_metadata);
}

namespace
{

ExpressionActionsPtr getCombinedIndicesExpression(
    const KeyDescription & key,
    const IndicesDescription & indices,
    const ColumnsDescription & columns,
    ContextPtr context)
{
    ASTPtr combined_expr_list = key.expression_list_ast->clone();

    for (const auto & index : indices)
        for (const auto & index_expr : index.expression_list_ast->children)
            combined_expr_list->children.push_back(index_expr->clone());

    auto syntax_result = TreeRewriter(context).analyze(combined_expr_list, columns.getAllPhysical());
    return ExpressionAnalyzer(combined_expr_list, syntax_result, context).getActions(false);
}

}

ExpressionActionsPtr MergeTreeMetaBase::getMinMaxExpr(const KeyDescription & partition_key, const ExpressionActionsSettings & settings)
{
    NamesAndTypesList partition_key_columns;
    if (!partition_key.column_names.empty())
        partition_key_columns = partition_key.expression->getRequiredColumnsWithTypes();

    return std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(partition_key_columns), settings);
}

Names MergeTreeMetaBase::getMinMaxColumnsNames(const KeyDescription & partition_key)
{
    if (!partition_key.column_names.empty())
        return partition_key.expression->getRequiredColumns();
    return {};
}

DataTypes MergeTreeMetaBase::getMinMaxColumnsTypes(const KeyDescription & partition_key)
{
    if (!partition_key.column_names.empty())
        return partition_key.expression->getRequiredColumnsWithTypes().getTypes();
    return {};
}

ExpressionActionsPtr MergeTreeMetaBase::getPrimaryKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot) const
{
    return getCombinedIndicesExpression(metadata_snapshot->getPrimaryKey(), metadata_snapshot->getSecondaryIndices(), metadata_snapshot->getColumns(), getContext());
}

ExpressionActionsPtr MergeTreeMetaBase::getSortingKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot) const
{
    return getCombinedIndicesExpression(metadata_snapshot->getSortingKey(), metadata_snapshot->getSecondaryIndices(), metadata_snapshot->getColumns(), getContext());
}

void MergeTreeMetaBase::checkPartitionKeyAndInitMinMax(const KeyDescription & new_partition_key)
{
    if (new_partition_key.expression_list_ast->children.empty())
        return;

    checkKeyExpression(*new_partition_key.expression, new_partition_key.sample_block, "Partition", allow_nullable_key);

    /// Add all columns used in the partition key to the min-max index.
    DataTypes minmax_idx_columns_types = getMinMaxColumnsTypes(new_partition_key);

    /// Try to find the date column in columns used by the partition key (a common case).
    /// If there are no - DateTime or DateTime64 would also suffice.

    bool has_date_column = false;
    bool has_datetime_column = false;

    for (size_t i = 0; i < minmax_idx_columns_types.size(); ++i)
    {
        if (isDate(minmax_idx_columns_types[i]))
        {
            if (!has_date_column)
            {
                minmax_idx_date_column_pos = i;
                has_date_column = true;
            }
            else
            {
                /// There is more than one Date column in partition key and we don't know which one to choose.
                minmax_idx_date_column_pos = -1;
            }
        }
    }
    if (!has_date_column)
    {
        for (size_t i = 0; i < minmax_idx_columns_types.size(); ++i)
        {
            if (isDateTime(minmax_idx_columns_types[i])
                || isDateTime64(minmax_idx_columns_types[i])
            )
            {
                if (!has_datetime_column)
                {
                    minmax_idx_time_column_pos = i;
                    has_datetime_column = true;
                }
                else
                {
                    /// There is more than one DateTime column in partition key and we don't know which one to choose.
                    minmax_idx_time_column_pos = -1;
                }
            }
        }
    }
}


void MergeTreeMetaBase::checkTTLExpressions(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata) const
{
    auto new_column_ttls = new_metadata.column_ttls_by_name;

    if (!new_column_ttls.empty())
    {
        NameSet columns_ttl_forbidden;

        if (old_metadata.hasPartitionKey())
            for (const auto & col : old_metadata.getColumnsRequiredForPartitionKey())
                columns_ttl_forbidden.insert(col);

        if (old_metadata.hasSortingKey())
            for (const auto & col : old_metadata.getColumnsRequiredForSortingKey())
                columns_ttl_forbidden.insert(col);

        for (const auto & [name, ttl_description] : new_column_ttls)
        {
            if (columns_ttl_forbidden.count(name))
                throw Exception("Trying to set TTL for key column " + name, ErrorCodes::ILLEGAL_COLUMN);
        }
    }
    auto new_table_ttl = new_metadata.table_ttl;

    if (new_table_ttl.definition_ast)
    {
        for (const auto & move_ttl : new_table_ttl.move_ttl)
        {
            if (move_ttl.destination_type == DataDestinationType::BYTECOOL)
                continue;

            if (!getDestinationForMoveTTL(move_ttl))
            {
                String message;
                if (move_ttl.destination_type == DataDestinationType::DISK)
                    message = "No such disk " + backQuote(move_ttl.destination_name) + " for given storage policy.";
                else
                    message = "No such volume " + backQuote(move_ttl.destination_name) + " for given storage policy.";
                throw Exception(message, ErrorCodes::BAD_TTL_EXPRESSION);
            }
        }
    }
}

MergeTreeMetaBase::PinnedPartUUIDsPtr MergeTreeMetaBase::getPinnedPartUUIDs() const
{
    std::lock_guard lock(pinned_part_uuids_mutex);
    return pinned_part_uuids;
}

bool MergeTreeMetaBase::canUsePolymorphicParts([[maybe_unused]]const MergeTreeSettings & settings, [[maybe_unused]]String * out_reason) const
{
    if (!canUseAdaptiveGranularity())
    {
        if (out_reason && (settings.min_rows_for_wide_part != 0 || settings.min_bytes_for_wide_part != 0
            || settings.min_rows_for_compact_part != 0 || settings.min_bytes_for_compact_part != 0))
        {
            *out_reason = fmt::format(
                    "Table can't create parts with adaptive granularity, but settings"
                    " min_rows_for_wide_part = {}"
                    ", min_bytes_for_wide_part = {}"
                    ", min_rows_for_compact_part = {}"
                    ", min_bytes_for_compact_part = {}"
                    ". Parts with non-adaptive granularity can be stored only in Wide (default) format.",
                    settings.min_rows_for_wide_part,    settings.min_bytes_for_wide_part,
                    settings.min_rows_for_compact_part, settings.min_bytes_for_compact_part);
        }

        return false;
    }

    return true;
}

MergeTreeMetaBase::AlterConversions MergeTreeMetaBase::getAlterConversionsForPart(const MergeTreeDataPartPtr part) const
{
    MutationCommands commands = getFirstAlterMutationCommandsForPart(part);

    AlterConversions result{};
    for (const auto & command : commands)
        /// Currently we need explicit conversions only for RENAME alter
        /// all other conversions can be deduced from diff between part columns
        /// and columns in storage.
        if (command.type == MutationCommand::Type::RENAME_COLUMN)
            result.rename_map[command.rename_to] = command.column_name;

    return result;
}

void MergeTreeMetaBase::addMutationEntry(const CnchMergeTreeMutationEntry & entry)
{
    std::lock_guard lock(mutations_by_version_mutex);
    mutations_by_version.try_emplace(entry.commit_time, entry);
}

void MergeTreeMetaBase::removeMutationEntry(TxnTimestamp create_time)
{
    std::lock_guard lock(mutations_by_version_mutex);
    /// Maybe erase all entries <= create_time?
    mutations_by_version.erase(create_time);
}

Strings MergeTreeMetaBase::getPlainMutationEntries()
{
    Strings res;
    std::lock_guard lock(mutations_by_version_mutex);
    res.reserve(mutations_by_version.size());
    for (auto const & [_, entry] : mutations_by_version)
    {
        res.push_back(entry.toString());
    }
    return res;
}

MergeTreeMetaBase::MutableDataPartPtr MergeTreeMetaBase::cloneAndLoadDataPartOnSameDisk(
    const MergeTreeMetaBase::DataPartPtr & src_part,
    const String & tmp_part_prefix,
    const MergeTreePartInfo & dst_part_info,
    const StorageMetadataPtr & metadata_snapshot)
{
    /// Check that the storage policy contains the disk where the src_part is located.
    bool does_storage_policy_allow_same_disk = false;
    for (const DiskPtr & disk : getStoragePolicy(IStorage::StorageLocation::MAIN)->getDisks())
    {
        if (disk->getName() == src_part->volume->getDisk()->getName())
        {
            does_storage_policy_allow_same_disk = true;
            break;
        }
    }
    if (!does_storage_policy_allow_same_disk)
        throw Exception(
            "Could not clone and load part " + quoteString(src_part->getFullPath()) + " because disk does not belong to storage policy",
            ErrorCodes::BAD_ARGUMENTS);

    String dst_part_name = src_part->getNewName(dst_part_info);
    String tmp_dst_part_name = tmp_part_prefix + dst_part_name;

    auto reservation = reserveSpace(src_part->getBytesOnDisk(), src_part->volume->getDisk());
    auto disk = reservation->getDisk();
    String src_part_path = src_part->getFullRelativePath();
    String dst_part_path = relative_data_path + tmp_dst_part_name;

    if (disk->exists(dst_part_path))
        throw Exception("Part in " + fullPath(disk, dst_part_path) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    /// If source part is in memory, flush it to disk and clone it already in on-disk format
    if (auto src_part_in_memory = asInMemoryPart(src_part))
    {
        const auto & src_relative_data_path = src_part_in_memory->storage.getRelativeDataPath(IStorage::StorageLocation::MAIN);
        auto flushed_part_path = src_part_in_memory->getRelativePathForPrefix(tmp_part_prefix, /*is_detach*/false);
        src_part_in_memory->flushToDisk(src_relative_data_path, flushed_part_path, metadata_snapshot);
        src_part_path = fs::path(src_relative_data_path) / flushed_part_path / "";
    }

    LOG_DEBUG(log, "Cloning part {} to {}", fullPath(disk, src_part_path), fullPath(disk, dst_part_path));
    localBackup(disk, src_part_path, dst_part_path);
    disk->removeFileIfExists(fs::path(dst_part_path) / IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME);

    auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);
    auto dst_data_part = createPart(dst_part_name, dst_part_info, single_disk_volume, tmp_dst_part_name);

    dst_data_part->is_temp = true;

    dst_data_part->loadColumnsChecksumsIndexes(require_part_metadata, true);
    dst_data_part->modification_time = disk->getLastModified(dst_part_path).epochTime();
    return dst_data_part;
}

String MergeTreeMetaBase::getFullPathOnDisk(StorageLocation location, const DiskPtr & disk) const
{
    return disk->getPath() + getRelativeDataPath(location);
}

bool MergeTreeMetaBase::supportsParallelInsert(ContextPtr local_context) const
{
    if (!getInMemoryMetadataPtr()->hasUniqueKey())
        return true;

    if (!local_context->getSettingsRef().optimize_unique_table_write)
        return false;
    return getSettings()->dedup_impl_version.value == DedupImplVersion::DEDUP_IN_TXN_COMMIT;
}

NamesAndTypesList MergeTreeMetaBase::getVirtuals() const
{
    /// Array(Tuple(String, String))
    static const auto map_column_keys_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));

    return NamesAndTypesList{
        NameAndTypePair("_part", std::make_shared<DataTypeString>()),
        NameAndTypePair("_part_index", std::make_shared<DataTypeUInt64>()),
        NameAndTypePair("_part_uuid", std::make_shared<DataTypeUUID>()),
        NameAndTypePair("_part_map_files", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())),
        NameAndTypePair("_map_column_keys", map_column_keys_type),
        NameAndTypePair("_partition_id", std::make_shared<DataTypeString>()),
        NameAndTypePair("_partition_value", getPartitionValueType()),
        NameAndTypePair("_sample_factor", std::make_shared<DataTypeFloat64>()),
        NameAndTypePair("_part_offset", std::make_shared<DataTypeUInt64>()),
        NameAndTypePair("_part_row_number", std::make_shared<DataTypeUInt64>()),
        NameAndTypePair("_bucket_number", std::make_shared<DataTypeInt64>()),
        RowExistsColumn::ROW_EXISTS_COLUMN,
    };
}

void MergeTreeMetaBase::insertQueryIdOrThrow(const String & query_id, size_t max_queries) const
{
    std::lock_guard lock(query_id_set_mutex);
    if (query_id_set.find(query_id) != query_id_set.end())
        return;
    if (query_id_set.size() >= max_queries)
        throw Exception(
            ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES, "Too many simultaneous queries for table {}. Maximum is: {}", log_name, max_queries);
    query_id_set.insert(query_id);
}

void MergeTreeMetaBase::removeQueryId(const String & query_id) const
{
    std::lock_guard lock(query_id_set_mutex);
    if (query_id_set.find(query_id) == query_id_set.end())
    {
        /// Do not throw exception, because this method is used in destructor.
        LOG_WARNING(log, "We have query_id removed but it's not recorded. This is a bug");
        assert(false);
    }
    else
        query_id_set.erase(query_id);
}

DataTypePtr MergeTreeMetaBase::getPartitionValueType() const
{
    DataTypePtr partition_value_type;
    auto partition_types = getInMemoryMetadataPtr()->partition_key.sample_block.getDataTypes();
    if (partition_types.empty())
        partition_value_type = std::make_shared<DataTypeUInt8>();
    else
        partition_value_type = std::make_shared<DataTypeTuple>(std::move(partition_types));
    return partition_value_type;
}

ASTs MergeTreeMetaBase::getPartVirtualExpr() const
{
    return {
        std::make_shared<ASTIdentifier>("_part"),
        std::make_shared<ASTIdentifier>("_partition_id"),
        std::make_shared<ASTIdentifier>("_part_uuid"),
        std::make_shared<ASTIdentifier>("_partition_value"),
        std::make_shared<ASTIdentifier>("_bucket_number")};
}

Block MergeTreeMetaBase::getSampleBlockWithVirtualColumns() const
{
    DataTypePtr partition_value_type = getPartitionValueType();
    return {
        ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "_part"),
        ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "_partition_id"),
        ColumnWithTypeAndName(ColumnUUID::create(), std::make_shared<DataTypeUUID>(), "_part_uuid"),
        ColumnWithTypeAndName(partition_value_type->createColumn(), partition_value_type, "_partition_value"),
        ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "_bucket_number")};
}

Block MergeTreeMetaBase::getBlockWithVirtualPartColumns(const DataPartsVector & parts, bool one_part) const
{
    DataTypePtr partition_value_type = getPartitionValueType();
    bool has_partition_value = typeid_cast<const DataTypeTuple *>(partition_value_type.get());
    Block block{
        ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "_part"),
        ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "_partition_id"),
        ColumnWithTypeAndName(ColumnUUID::create(), std::make_shared<DataTypeUUID>(), "_part_uuid"),
        ColumnWithTypeAndName(partition_value_type->createColumn(), partition_value_type, "_partition_value"),
        ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "_bucket_number")};

    MutableColumns columns = block.mutateColumns();

    auto & part_column = columns[0];
    auto & partition_id_column = columns[1];
    auto & part_uuid_column = columns[2];
    auto & partition_value_column = columns[3];
    auto & bucket_number_column = columns[4];

    for (const auto & part_or_projection : parts)
    {
        const auto * part = part_or_projection->isProjectionPart() ? part_or_projection->getParentPart() : part_or_projection.get();
        part_column->insert(part->name);
        partition_id_column->insert(part->info.partition_id);
        part_uuid_column->insert(part->uuid);
        bucket_number_column->insert(part->bucket_number);
        // coverity[mismatched_iterator]
        Tuple tuple(part->partition.value.begin(), part->partition.value.end());
        if (has_partition_value)
            partition_value_column->insert(tuple);

        if (one_part)
        {
            part_column = ColumnConst::create(std::move(part_column), 1);
            partition_id_column = ColumnConst::create(std::move(partition_id_column), 1);
            part_uuid_column = ColumnConst::create(std::move(part_uuid_column), 1);
            bucket_number_column = ColumnConst::create(std::move(bucket_number_column), 1);
            if (has_partition_value)
                partition_value_column = ColumnConst::create(std::move(partition_value_column), 1);
            break;
        }
    }

    block.setColumns(std::move(columns));
    if (!has_partition_value)
        block.erase("_partition_value");
    return block;
}

Block MergeTreeMetaBase::getPartitionBlockWithVirtualColumns(
    const std::vector<std::shared_ptr<MergeTreePartition>> & partition_list) const
{
    auto block = getInMemoryMetadataPtr()->partition_key.sample_block;
    DataTypePtr partition_value_type = getPartitionValueType();
    block.insert(ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "_partition_id"));
    block.insert(ColumnWithTypeAndName(partition_value_type->createColumn(), partition_value_type, "_partition_value"));

    MutableColumns columns = block.mutateColumns();

    bool has_partition_value = typeid_cast<const DataTypeTuple *>(partition_value_type.get());
    auto block_size = block.columns();

    auto & partition_id_column = columns[block_size-2];
    auto & partition_value_column = columns[block_size-1];

    std::for_each(columns.begin(), columns.end(), [&](auto & column) { column->reserve(partition_list.size()); });

    for (const auto & partition : partition_list)
    {
        partition_id_column->insert(partition->getID(*this));
        if (has_partition_value)
        {
            for (size_t i = 0; i < partition->value.size(); i++)
                columns[i]->insert(partition->value[i]);
            Tuple tuple(partition->value.begin(), partition->value.end());
            partition_value_column->insert(std::move(tuple));
        }
    }
    block.setColumns(std::move(columns));
    if (!has_partition_value)
        block.erase(block.getPositionByName("_partition_value"));
    return block;
}

MergeTreeDataPartType MergeTreeMetaBase::choosePartType(size_t bytes_uncompressed, size_t rows_count) const
{
    // FIXME (UNIQUE KEY): for altering unique table, we only expect the part to be wide
    if (getInMemoryMetadataPtr()->hasUniqueKey())
        return MergeTreeDataPartType::WIDE;

    const auto settings = getSettings();
    if (settings->enable_ingest_wide_part)
        return MergeTreeDataPartType::WIDE;

    if (!canUsePolymorphicParts(*settings))
        return MergeTreeDataPartType::WIDE;

    if (bytes_uncompressed < settings->min_bytes_for_compact_part || rows_count < settings->min_rows_for_compact_part)
        return MergeTreeDataPartType::IN_MEMORY;

    if (bytes_uncompressed < settings->min_bytes_for_wide_part || rows_count < settings->min_rows_for_wide_part)
        return MergeTreeDataPartType::COMPACT;

    return MergeTreeDataPartType::WIDE;
}

MergeTreeDataPartType MergeTreeMetaBase::choosePartTypeOnDisk(size_t bytes_uncompressed, size_t rows_count) const
{
    const auto settings = getSettings();
    if (!canUsePolymorphicParts(*settings))
        return MergeTreeDataPartType::WIDE;

    if (bytes_uncompressed < settings->min_bytes_for_wide_part || rows_count < settings->min_rows_for_wide_part)
        return MergeTreeDataPartType::COMPACT;

    return MergeTreeDataPartType::WIDE;
}


MergeTreeMetaBase::MutableDataPartPtr MergeTreeMetaBase::createPart(
    const String & name,
    MergeTreeDataPartType type,
    const MergeTreePartInfo & part_info,
    const VolumePtr & volume,
    const String & relative_path,
    const IMergeTreeDataPart * parent_part,
    StorageLocation location) const
{
    switch (type.getValue())
    {
        case MergeTreeDataPartType::COMPACT:
            return std::make_shared<MergeTreeDataPartCompact>(*this, name, part_info, volume, relative_path, parent_part, location);
        case MergeTreeDataPartType::WIDE:
            return std::make_shared<MergeTreeDataPartWide>(*this, name, part_info, volume, relative_path, parent_part, location);
        case MergeTreeDataPartType::IN_MEMORY:
            return std::make_shared<MergeTreeDataPartInMemory>(*this, name, part_info, volume, relative_path, parent_part, location);
        case MergeTreeDataPartType::CNCH:
            if (location != StorageLocation::MAIN)
            {
                throw Exception("Create CNCH part in auxility storage is forbidden",
                    ErrorCodes::LOGICAL_ERROR);
            }
            return std::make_shared<MergeTreeDataPartCNCH>(*this, name, part_info, volume, relative_path, parent_part);
        case MergeTreeDataPartType::UNKNOWN:
            throw Exception("Unknown type of part " + relative_path, ErrorCodes::UNKNOWN_PART_TYPE);
    }

    __builtin_unreachable();
}

static MergeTreeDataPartType getPartTypeFromMarkExtension(const String & mrk_ext)
{
    if (mrk_ext == getNonAdaptiveMrkExtension())
        return MergeTreeDataPartType::WIDE;
    if (mrk_ext == getAdaptiveMrkExtension(MergeTreeDataPartType::WIDE))
        return MergeTreeDataPartType::WIDE;
    if (mrk_ext == getAdaptiveMrkExtension(MergeTreeDataPartType::COMPACT))
        return MergeTreeDataPartType::COMPACT;

    throw Exception("Can't determine part type, because of unknown mark extension " + mrk_ext, ErrorCodes::UNKNOWN_PART_TYPE);
}

MergeTreeMetaBase::MutableDataPartPtr MergeTreeMetaBase::createPart(const String & name,
    const VolumePtr & volume, const String & relative_path, const IMergeTreeDataPart * parent_part,
    StorageLocation location) const
{
    return createPart(name, MergeTreePartInfo::fromPartName(name, format_version),
        volume, relative_path, parent_part, location);
}

MergeTreeMetaBase::MutableDataPartPtr MergeTreeMetaBase::createPart(const String & name,
    const MergeTreePartInfo & part_info, const VolumePtr & volume,
    const String & relative_path, const IMergeTreeDataPart * parent_part,
    StorageLocation location) const
{
    MergeTreeDataPartType type;
    auto full_path = fs::path(getRelativeDataPath(location)) / (parent_part ? parent_part->relative_path : "") / relative_path / "";
    auto mrk_ext = MergeTreeIndexGranularityInfo::getMarksExtensionFromFilesystem(volume->getDisk(), full_path);

    if (mrk_ext)
        type = getPartTypeFromMarkExtension(*mrk_ext);
    else
    {
        /// Didn't find any mark file, suppose that part is empty.
        type = choosePartTypeOnDisk(0, 0);
    }

    return createPart(name, type, part_info, volume, relative_path, parent_part, location);
}

/// TODO: what is the performance of get parts info from part cache manager
std::pair<Int64, Int64> MergeTreeMetaBase::getCnchPartsInfo() const
{
    try
    {
        auto part_cache_manager = getContext()->getPartCacheManager();
        if (part_cache_manager)
            return part_cache_manager->getTotalAndMaxPartsNumber(*this);
    }
    catch (...)
    {
        tryLogCurrentException(__FUNCTION__, "Failed to get parts info from part cache manager");
    }

    /// Just return {0, 0} to skip this time check
    return {0, 0};
}

void MergeTreeMetaBase::cnchDelayInsertOrThrowIfNeeded() const
{
    Stopwatch stop_watch;
    SCOPE_EXIT_SAFE({
        if (stop_watch.elapsedMilliseconds() > 500)
            LOG_INFO(log, "Delay insert check took {} ms", stop_watch.elapsedMilliseconds());
    });

    const auto settings = getSettings();
    const Int64 allowed_max_parts_in_total = settings->max_parts_in_total;
    const Int64 allowed_parts_to_throw_insert = settings->parts_to_throw_insert;

    /// Return instantly to avoid getPartsInfo if no parts check set
    if (allowed_max_parts_in_total == 0 && allowed_parts_to_throw_insert == 0)
        return;

    auto host_ports = getContext()->getCnchTopologyMaster()->getTargetServer(
                                                            UUIDHelpers::UUIDToString(getCnchStorageUUID()),
                                                             getServerVwName(), true);
    if (!host_ports.empty() && !isLocalServer(host_ports.getRPCAddress(), std::to_string(getContext()->getRPCPort())))
    {
        auto server_client = getContext()->getCnchServerClient(host_ports);
        if (server_client)
            server_client->checkDelayInsertOrThrowIfNeeded(getStorageUUID());
        else
            LOG_WARNING(log, "Failed to get target server {} while checking delay insert", host_ports.getRPCAddress());

        return;
    }

    auto [parts_count_in_total, parts_count_in_partition] = getCnchPartsInfo();
    if (allowed_max_parts_in_total > 0 && parts_count_in_total >= allowed_max_parts_in_total)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception(ErrorCodes::TOO_MANY_PARTS,
                "Too many parts (" + toString(parts_count_in_total) + ") in all partitions in total. "
                + "This indicates wrong choice of partition key. The threshold can be modified with 'max_parts_in_total' setting "
                + "in <merge_tree> element in config.xml or with per-table setting.");
    }

    if (allowed_parts_to_throw_insert > 0 && parts_count_in_partition >= allowed_parts_to_throw_insert)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception(
            ErrorCodes::TOO_MANY_PARTS,
            "Too many parts (" + toString(parts_count_in_partition) + ") in partition. Merges are processing "
            + "significantly slower than inserts. The threshold can be modified with 'parts_to_throw_insert' setting.");
    }
}

MergeTreeMetaBase::DataParts MergeTreeMetaBase::getDataParts(const DataPartStates & affordable_states) const
{
    DataParts res;
    {
        auto lock = lockPartsRead();
        for (auto state : affordable_states)
        {
            auto range = getDataPartsStateRange(state);
            res.insert(range.begin(), range.end());
        }
    }
    return res;
}

MergeTreeMetaBase::DataPartsVector MergeTreeMetaBase::getDataPartsVectorUnlocked(
    const DataPartStates & affordable_states,
    const DataPartsLock & /*lock*/,
    DataPartStateVector * out_states,
    bool require_projection_parts) const
{
    DataPartsVector res;
    DataPartsVector buf;

    for (auto state : affordable_states)
    {
        auto range = getDataPartsStateRange(state);

        if (require_projection_parts)
        {
            for (const auto & part : range)
            {
                for (const auto & [_, projection_part] : part->getProjectionParts())
                    res.push_back(projection_part);
            }
        }
        else
        {
            std::swap(buf, res);
            res.clear();
            std::merge(range.begin(), range.end(), buf.begin(), buf.end(), std::back_inserter(res), LessDataPart()); //-V783
        }
    }

    if (out_states != nullptr)
    {
        out_states->resize(res.size());
        if (require_projection_parts)
        {
            for (size_t i = 0; i < res.size(); ++i)
                (*out_states)[i] = res[i]->getParentPart()->getState();
        }
        else
        {
            for (size_t i = 0; i < res.size(); ++i)
                (*out_states)[i] = res[i]->getState();
        }
    }

    return res;
}

MergeTreeMetaBase::DataPartsVector MergeTreeMetaBase::getDataPartsVector(
    const DataPartStates & affordable_states, DataPartStateVector * out_states, bool require_projection_parts) const
{
    DataPartsVector res;
    DataPartsVector buf;
    {
        auto lock = lockPartsRead();

        for (auto state : affordable_states)
        {
            auto range = getDataPartsStateRange(state);

            if (require_projection_parts)
            {
                for (const auto & part : range)
                {
                    for (const auto & [p_name, projection_part] : part->getProjectionParts())
                        res.push_back(projection_part);
                }
            }
            else
            {
                std::swap(buf, res);
                res.clear();
                std::merge(range.begin(), range.end(), buf.begin(), buf.end(), std::back_inserter(res), LessDataPart()); //-V783
            }
        }

        if (out_states != nullptr)
        {
            out_states->resize(res.size());
            if (require_projection_parts)
            {
                for (size_t i = 0; i < res.size(); ++i)
                    (*out_states)[i] = res[i]->getParentPart()->getState();
            }
            else
            {
                for (size_t i = 0; i < res.size(); ++i)
                    (*out_states)[i] = res[i]->getState();
            }
        }
    }

    return res;
}

MergeTreeMetaBase::DataPartsVector
MergeTreeMetaBase::getDataPartsVectorInPartition(MergeTreeMetaBase::DataPartState state, const String & partition_id) const
{
    DataPartStateAndPartitionID state_with_partition{state, partition_id};

    auto lock = lockPartsRead();
    return DataPartsVector(
        data_parts_by_state_and_info.lower_bound(state_with_partition), data_parts_by_state_and_info.upper_bound(state_with_partition));
}

MergeTreeMetaBase::DataParts MergeTreeMetaBase::getDataParts() const
{
    return getDataParts({DataPartState::Committed});
}

MergeTreeMetaBase::DataPartsVector MergeTreeMetaBase::getDataPartsVector() const
{
    return getDataPartsVector({DataPartState::Committed});
}

MergeTreeMetaBase::DataPartPtr MergeTreeMetaBase::getPartIfExists(const MergeTreePartInfo & part_info, const MergeTreeMetaBase::DataPartStates & valid_states)
{
    auto lock = lockPartsRead();
    return getPartIfExistsWithoutLock(part_info, valid_states);
}

MergeTreeMetaBase::DataPartPtr MergeTreeMetaBase::getPartIfExistsWithoutLock(const MergeTreePartInfo & part_info, const MergeTreeMetaBase::DataPartStates & valid_states)
{
    auto it = data_parts_by_info.find(part_info);
    if (it == data_parts_by_info.end())
        return nullptr;

    for (auto state : valid_states)
        if ((*it)->getState() == state)
            return *it;

    return nullptr;
}

MergeTreeMetaBase::DataPartPtr MergeTreeMetaBase::getPartIfExists(const String & part_name, const MergeTreeMetaBase::DataPartStates & valid_states)
{
    return getPartIfExists(MergeTreePartInfo::fromPartName(part_name, format_version), valid_states);
}
bool MergeTreeMetaBase::hasPart(const String & part_name, const MergeTreeMetaBase::DataPartStates & valid_states)
{
    const MergeTreePartInfo & part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
    return hasPart(part_info, valid_states);
}

bool MergeTreeMetaBase::hasPart(const MergeTreePartInfo & part_info, const MergeTreeMetaBase::DataPartStates & valid_states)
{
    // TODO dongyifeng change to lockPartsRead later
    auto lock = lockParts();

    auto it = data_parts_by_info.find(part_info);
    if (it == data_parts_by_info.end())
        return false;
    for (auto state : valid_states)
    {
        if ((*it)->getState() == state)
            return true;
    }
    return false;
}

MergeTreeMetaBase::DataPartPtr MergeTreeMetaBase::getPartIfExistsWithoutLock(const String & part_name, const MergeTreeMetaBase::DataPartStates & valid_states)
{
    return getPartIfExistsWithoutLock(MergeTreePartInfo::fromPartName(part_name, format_version), valid_states);
}

String MergeTreeMetaBase::getPartitionIDFromQuery(const ASTPtr & ast, ContextPtr local_context) const
{
    if (const auto & ast_literal = ast->as<ASTLiteral>())
    {
        return ast_literal->value.get<String>();
    }

    const auto & partition_ast = ast->as<ASTPartition &>();

    if (!partition_ast.value)
    {
        MergeTreePartInfo::validatePartitionID(partition_ast.id, format_version);
        return partition_ast.id;
    }

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// Month-partitioning specific - partition ID can be passed in the partition value.
        const auto * partition_lit = partition_ast.value->as<ASTLiteral>();
        if (partition_lit && partition_lit->value.getType() == Field::Types::String)
        {
            String partition_id = partition_lit->value.get<String>();
            MergeTreePartInfo::validatePartitionID(partition_id, format_version);
            return partition_id;
        }
    }

    /// Re-parse partition key fields using the information about expected field types.

    auto metadata_snapshot = getInMemoryMetadataPtr();
    size_t fields_count = metadata_snapshot->getPartitionKey().sample_block.columns();
    if (partition_ast.fields_count != fields_count)
        throw Exception(
            "Wrong number of fields in the partition expression: " + toString(partition_ast.fields_count) +
            ", must be: " + toString(fields_count),
            ErrorCodes::INVALID_PARTITION_VALUE);

    const FormatSettings format_settings;
    Row partition_row(fields_count);

    if (fields_count)
    {
        ReadBufferFromMemory left_paren_buf("(", 1);
        ReadBufferFromMemory fields_buf(partition_ast.fields_str.data(), partition_ast.fields_str.size());
        ReadBufferFromMemory right_paren_buf(")", 1);
        ConcatReadBuffer buf({&left_paren_buf, &fields_buf, &right_paren_buf});

        auto input_format = FormatFactory::instance().getInput(
            "Values",
            buf,
            metadata_snapshot->getPartitionKey().sample_block,
            local_context,
            local_context->getSettingsRef().max_block_size);
        auto input_stream = std::make_shared<InputStreamFromInputFormat>(input_format);

        auto block = input_stream->read();
        if (!block || !block.rows())
            throw Exception(
                "Could not parse partition value: `" + partition_ast.fields_str + "`",
                ErrorCodes::INVALID_PARTITION_VALUE);

        for (size_t i = 0; i < fields_count; ++i)
            block.getByPosition(i).column->get(0, partition_row[i]);
    }

    MergeTreePartition partition(std::move(partition_row));
    String partition_id = partition.getID(*this);

    {
        auto data_parts_lock = lockParts();
        DataPartPtr existing_part_in_partition = getAnyPartInPartition(partition_id, data_parts_lock);
        if (existing_part_in_partition && existing_part_in_partition->partition.value != partition.value)
        {
            WriteBufferFromOwnString buf;
            writeCString("Parsed partition value: ", buf);
            partition.serializeText(*this, buf, format_settings);
            writeCString(" doesn't match partition value for an existing part with the same partition ID: ", buf);
            writeString(existing_part_in_partition->name, buf);
            throw Exception(buf.str(), ErrorCodes::INVALID_PARTITION_VALUE);
        }
    }

    return partition_id;
}

namespace
{

inline ReservationPtr checkAndReturnReservation(UInt64 expected_size, ReservationPtr reservation)
{
    if (reservation)
        return reservation;

    throw Exception(fmt::format("Cannot reserve {}, not enough space", ReadableSize(expected_size)), ErrorCodes::NOT_ENOUGH_SPACE);
}

}

ReservationPtr MergeTreeMetaBase::reserveSpace(UInt64 expected_size, StorageLocation location) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    return getStoragePolicy(location)->reserveAndCheck(expected_size);
}

ReservationPtr MergeTreeMetaBase::reserveSpace(UInt64 expected_size, SpacePtr space)
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    auto reservation = tryReserveSpace(expected_size, space);
    return checkAndReturnReservation(expected_size, std::move(reservation));
}

ReservationPtr MergeTreeMetaBase::tryReserveSpace(UInt64 expected_size, SpacePtr space)
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    return space->reserve(expected_size);
}

ReservationPtr MergeTreeMetaBase::reserveSpacePreferringTTLRules(
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 expected_size,
    const IMergeTreeDataPart::TTLInfos & ttl_infos,
    time_t time_of_move,
    size_t min_volume_index,
    bool is_insert,
    DiskPtr selected_disk,
    StorageLocation location) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    ReservationPtr reservation = tryReserveSpacePreferringTTLRules(
        metadata_snapshot, expected_size, ttl_infos, time_of_move, min_volume_index, is_insert, selected_disk, location);

    return checkAndReturnReservation(expected_size, std::move(reservation));
}

ReservationPtr MergeTreeMetaBase::tryReserveSpacePreferringTTLRules(
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 expected_size,
    const IMergeTreeDataPart::TTLInfos & ttl_infos,
    time_t time_of_move,
    size_t min_volume_index,
    bool is_insert,
    DiskPtr selected_disk,
    StorageLocation location) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    ReservationPtr reservation;

    auto move_ttl_entry = selectTTLDescriptionForTTLInfos(metadata_snapshot->getMoveTTLs(), ttl_infos.moves_ttl, time_of_move, true);

    if (move_ttl_entry)
    {
        SpacePtr destination_ptr = getDestinationForMoveTTL(*move_ttl_entry, is_insert, location);
        if (!destination_ptr)
        {
            if (move_ttl_entry->destination_type == DataDestinationType::VOLUME)
                LOG_WARNING(log, "Would like to reserve space on volume '{}' by TTL rule of table '{}' but volume was not found or rule is not applicable at the moment",
                    move_ttl_entry->destination_name, log_name);
            else if (move_ttl_entry->destination_type == DataDestinationType::DISK)
                LOG_WARNING(log, "Would like to reserve space on disk '{}' by TTL rule of table '{}' but disk was not found or rule is not applicable at the moment",
                    move_ttl_entry->destination_name, log_name);
        }
        else
        {
            reservation = destination_ptr->reserve(expected_size);
            if (reservation)
                return reservation;
            else
                if (move_ttl_entry->destination_type == DataDestinationType::VOLUME)
                    LOG_WARNING(log, "Would like to reserve space on volume '{}' by TTL rule of table '{}' but there is not enough space",
                    move_ttl_entry->destination_name, log_name);
                else if (move_ttl_entry->destination_type == DataDestinationType::DISK)
                    LOG_WARNING(log, "Would like to reserve space on disk '{}' by TTL rule of table '{}' but there is not enough space",
                        move_ttl_entry->destination_name, log_name);
        }
    }

    // Prefer selected_disk
    if (selected_disk)
        reservation = selected_disk->reserve(expected_size);

    if (!reservation)
        reservation = getStoragePolicy(location)->reserve(expected_size, min_volume_index);

    return reservation;
}

SpacePtr MergeTreeMetaBase::getDestinationForMoveTTL(const TTLDescription & move_ttl, bool is_insert, StorageLocation location) const
{
    auto policy = getStoragePolicy(location);
    if (move_ttl.destination_type == DataDestinationType::VOLUME)
    {
        auto volume = policy->getVolumeByName(move_ttl.destination_name, true);

        if (!volume)
            return {};

        if (is_insert && !volume->perform_ttl_move_on_insert)
            return {};

        return volume;
    }
    else if (move_ttl.destination_type == DataDestinationType::DISK)
    {
        auto disk = policy->getDiskByName(move_ttl.destination_name);
        if (!disk)
            return {};

        auto volume = policy->getVolume(policy->getVolumeIndexByDisk(disk));
        if (!volume)
            return {};

        if (is_insert && !volume->perform_ttl_move_on_insert)
            return {};

        return disk;
    }
    else
        return {};
}

bool MergeTreeMetaBase::isPartInTTLDestination(const TTLDescription & ttl, const IMergeTreeDataPart & part) const
{
    auto policy = getStoragePolicy(IStorage::StorageLocation::MAIN);
    if (ttl.destination_type == DataDestinationType::VOLUME)
    {
        for (const auto & disk : policy->getVolumeByName(ttl.destination_name, true)->getDisks())
            if (disk->getName() == part.volume->getDisk()->getName())
                return true;
    }
    else if (ttl.destination_type == DataDestinationType::DISK)
        return policy->getDiskByName(ttl.destination_name)->getName() == part.volume->getDisk()->getName();
    return false;
}

CompressionCodecPtr MergeTreeMetaBase::getCompressionCodecForPart(size_t part_size_compressed, const IMergeTreeDataPart::TTLInfos & ttl_infos, time_t current_time) const
{

    auto metadata_snapshot = getInMemoryMetadataPtr();

    const auto & recompression_ttl_entries = metadata_snapshot->getRecompressionTTLs();
    auto best_ttl_entry = selectTTLDescriptionForTTLInfos(recompression_ttl_entries, ttl_infos.recompression_ttl, current_time, true);


    if (best_ttl_entry)
        return CompressionCodecFactory::instance().get(best_ttl_entry->recompression_codec, {});

    return getContext()->chooseCompressionCodec(
        part_size_compressed,
        static_cast<double>(part_size_compressed) / getTotalActiveSizeInBytes());
}

MergeTreeMetaBase::DataPartPtr MergeTreeMetaBase::getAnyPartInPartition(
    const String & partition_id, DataPartsLock & /*data_parts_lock*/) const
{
    auto it = data_parts_by_state_and_info.lower_bound(DataPartStateAndPartitionID{DataPartState::Committed, partition_id});

    while (it != data_parts_by_state_and_info.end() && (*it)->getState() == DataPartState::Committed
           && (*it)->info.partition_id == partition_id)
    {
        if ((*it)->info.isFakeDropRangePart())
        {
            ++it;
            continue;
        }
        return *it;
    }

    // if (it != data_parts_by_state_and_info.end() && (*it)->getState() == DataPartState::Committed && (*it)->info.partition_id == partition_id)
    //     return *it;

    return nullptr;
}

void MergeTreeMetaBase::MergingParams::check(const StorageInMemoryMetadata & metadata, bool attach) const
{
    const bool has_unique_key = metadata.hasUniqueKey();
    const auto columns = metadata.getColumns().getAllPhysical();

    if (!sign_column.empty() && mode != MergingParams::Collapsing && mode != MergingParams::VersionedCollapsing)
        throw Exception("Sign column for MergeTree cannot be specified in modes except Collapsing or VersionedCollapsing.",
                        ErrorCodes::LOGICAL_ERROR);

    if (!has_unique_key && !version_column.empty() && mode != MergingParams::Replacing && mode != MergingParams::VersionedCollapsing)
        throw Exception("Version column for MergeTree cannot be specified in modes except Replacing or VersionedCollapsing.",
                        ErrorCodes::LOGICAL_ERROR);

    if (!columns_to_sum.empty() && mode != MergingParams::Summing)
        throw Exception("List of columns to sum for MergeTree cannot be specified in all modes except Summing.",
                        ErrorCodes::LOGICAL_ERROR);

    /// Check that if the sign column is needed, it exists and is of type Int8.
    auto check_sign_column = [this, & columns](bool is_optional, const std::string & storage)
    {
        if (sign_column.empty())
        {
            if (is_optional)
                return;

            throw Exception("Logical error: Sign column for storage " + storage + " is empty", ErrorCodes::LOGICAL_ERROR);
        }

        bool miss_column = true;
        for (const auto & column : columns)
        {
            if (column.name == sign_column)
            {
                if (!typeid_cast<const DataTypeInt8 *>(column.type.get()))
                    throw Exception("Sign column (" + sign_column + ") for storage " + storage + " must have type Int8."
                            " Provided column of type " + column.type->getName() + ".", ErrorCodes::BAD_TYPE_OF_FIELD);
                miss_column = false;
                break;
            }
        }
        if (miss_column)
            throw Exception("Sign column " + sign_column + " does not exist in table declaration.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    };

    /// that if the version_column column is needed, it exists and is of unsigned integer type.
    auto check_version_column = [this, & columns](bool is_optional, const std::string & storage)
    {
        if (version_column.empty())
        {
            if (is_optional)
                return;

            throw Exception("Logical error: Version column for storage " + storage + " is empty", ErrorCodes::LOGICAL_ERROR);
        }

        bool miss_column = true;
        for (const auto & column : columns)
        {
            if (column.name == version_column)
            {
                if (!column.type->canBeUsedAsVersion())
                    throw Exception("The column " + version_column +
                        " cannot be used as a version column for storage " + storage +
                        " because it is of type " + column.type->getName() +
                        " (must be of an integer type or of type Date/DateTime/DateTime64)", ErrorCodes::BAD_TYPE_OF_FIELD);
                miss_column = false;
                break;
            }
        }
        if (miss_column)
            throw Exception("Version column " + version_column + " does not exist in table declaration.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    };

    if (mode == MergingParams::Collapsing)
        check_sign_column(false, "CollapsingMergeTree");

    if (mode == MergingParams::Summing)
    {
        /// If columns_to_sum are set, then check that such columns exist.
        for (const auto & column_to_sum : columns_to_sum)
        {
            auto check_column_to_sum_exists = [& column_to_sum](const NameAndTypePair & name_and_type)
            {
                return column_to_sum == Nested::extractTableName(name_and_type.name);
            };
            if (columns.end() == std::find_if(columns.begin(), columns.end(), check_column_to_sum_exists))
                throw Exception(
                        "Column " + column_to_sum + " listed in columns to sum does not exist in table declaration.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
        }

        /// Check that summing columns are not in partition key.
        if (metadata.isPartitionKeyDefined())
        {
            auto partition_key_columns = metadata.getPartitionKey().column_names;

            Names names_intersection;
            std::set_intersection(columns_to_sum.begin(), columns_to_sum.end(),
                                  partition_key_columns.begin(), partition_key_columns.end(),
                                  std::back_inserter(names_intersection));

            if (!names_intersection.empty())
                throw Exception("Columns: " + boost::algorithm::join(names_intersection, ", ") +
                " listed both in columns to sum and in partition key. That is not allowed.", ErrorCodes::BAD_ARGUMENTS);
        }
    }


    if (has_unique_key)
    {
        if (partitionValueAsVersion())
        {
            if (metadata.partition_key.sample_block.columns() == 0)
                throw Exception("Table is not partitioned, can't use partition value as version", ErrorCodes::BAD_ARGUMENTS);
            if (metadata.partition_key.sample_block.columns() > 1)
                throw Exception("Partition key contains more than one column, can't use it as version", ErrorCodes::BAD_ARGUMENTS);
            auto partition_key_type = metadata.partition_key.sample_block.getDataTypes()[0];
            if (!partition_key_type->canBeUsedAsVersion())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition key has type {}, can't be used as version", partition_key_type->getName());
            // singed integer and types larger than 64 bits are not supported currently
            if (!attach && TypeIndex::UInt64 < partition_key_type->getTypeId() && partition_key_type->getTypeId() <= TypeIndex::Int256)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition key has type {}, can't be used as version", partition_key_type->getName());
        }
        else
        {
            check_version_column(true, "Unique Key");
        }
    }
    else if (partitionValueAsVersion())
    {
        throw Exception("Table doesn't have UNIQUE KEY, can't use partition value as version", ErrorCodes::BAD_ARGUMENTS);
    }

    if (mode == MergingParams::Replacing)
        check_version_column(true, "ReplacingMergeTree");

    if (mode == MergingParams::VersionedCollapsing)
    {
        check_sign_column(false, "VersionedCollapsingMergeTree");
        check_version_column(false, "VersionedCollapsingMergeTree");
    }

    /// TODO Checks for Graphite mode.
}

String MergeTreeMetaBase::MergingParams::getModeName() const
{
    switch (mode)
    {
        case Ordinary:      return "";
        case Collapsing:    return "Collapsing";
        case Summing:       return "Summing";
        case Aggregating:   return "Aggregating";
        case Replacing:     return "Replacing";
        case Graphite:      return "Graphite";
        case VersionedCollapsing: return "VersionedCollapsing";
    }

    __builtin_unreachable();
}

String MergeTreeMetaBase::getStorageUniqueID() const
{
    if (getStorageID().hasUUID())
        return toString(getStorageID().uuid);
    else
        return storage_address;
}

UUID MergeTreeMetaBase::getCnchStorageUUID() const
{
    if (getStorageID().hasUUID())
        return getStorageID().uuid;
    else
    {
        /// Get cnch table uuid from settings as CloudMergeTree has no uuid for Kafka task
        String uuid_str = getSettings()->cnch_table_uuid.value;
        if (!uuid_str.empty())
            return UUIDHelpers::toUUID(uuid_str);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage uuid of table {} can't be empty", getStorageID().getNameForLogs());
    }
}

using DataPart = IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const DataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

std::pair<DataPartsVector, double> MergeTreeMetaBase::getCalculatedPartsAndRatio() const
{
    /// Take into account only committed parts
    auto committed_parts_range = getDataPartsStateRange(DataPartState::Committed);
    DataPartsVector committed_parts{committed_parts_range.begin(), committed_parts_range.end()};
    DataPartsVector calculated_parts;

    double ratio = 1.0;
    size_t sample_number = getContext()->getSettingsRef().merge_tree_calculate_columns_size_sample;
    if (getSettings()->enable_calculate_columns_size_with_sample == 1 && committed_parts.size() > sample_number*2)
    {
        calculated_parts.reserve(sample_number);
        std::sample(committed_parts.begin(), committed_parts.end(),
                    std::back_inserter(calculated_parts), sample_number,
                    std::mt19937{std::random_device{}()});
        size_t m = committed_parts.size() / sample_number;
        ratio = m + static_cast<double>(committed_parts.size() - m * sample_number) / sample_number;
    }
    else
    {
        calculated_parts = committed_parts;
    }
    return std::make_pair(calculated_parts, ratio);
}

void MergeTreeMetaBase::calculateColumnSizesImpl()
{
    Stopwatch stopwatch;

    column_sizes.clear();

    auto parts_and_ratio = getCalculatedPartsAndRatio();
    DataPartsVector calculated_parts = parts_and_ratio.first;
    double ratio = parts_and_ratio.second;

    for (const auto & part : calculated_parts)
        addPartContributionToColumnSizes(part);

    if (ratio > 1.0)
    {
        ColumnSizeByName new_column_sizes = column_sizes;
        /// Adjust size according to the sample ratio
        for (auto & pair : new_column_sizes)
        {
            pair.second.marks = pair.second.marks * ratio;
            pair.second.data_compressed = pair.second.data_compressed * ratio;
            pair.second.data_uncompressed = pair.second.data_uncompressed * ratio;
        }

        column_sizes = std::move(new_column_sizes);
    }

    LOG_DEBUG(log, "Calculate columns size elapsed: {} ms.", stopwatch.elapsedMilliseconds());
}


void MergeTreeMetaBase::addPartContributionToColumnSizes(const DataPartPtr & part)
{
    for (const auto & column : part->getColumns())
    {
        ColumnSize & total_column_size = column_sizes[column.name];
        ColumnSize part_column_size = part->getColumnSize(column.name, *column.type);
        total_column_size.add(part_column_size);
    }
}

void MergeTreeMetaBase::removePartContributionToColumnSizes(const DataPartPtr & part)
{
    for (const auto & column : part->getColumns())
    {
        ColumnSize & total_column_size = column_sizes[column.name];
        ColumnSize part_column_size = part->getColumnSize(column.name, *column.type);

        auto log_subtract = [&](size_t & from, size_t value, const char * field)
        {
            if (value > from)
                LOG_ERROR(log, "Possibly incorrect column size subtraction: {} - {} = {}, column: {}, field: {}",
                    from, value, from - value, column.name, field);

            from -= value;
        };

        log_subtract(total_column_size.data_compressed, part_column_size.data_compressed, ".data_compressed");
        log_subtract(total_column_size.data_uncompressed, part_column_size.data_uncompressed, ".data_uncompressed");
        log_subtract(total_column_size.marks, part_column_size.marks, ".marks");
    }
}

bool MergeTreeMetaBase::isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(
    const ASTPtr & node, const StorageMetadataPtr & metadata_snapshot) const
{
    const String column_name = node->getColumnName();

    for (const auto & name : metadata_snapshot->getPrimaryKeyColumns())
        if (column_name == name)
            return true;

    for (const auto & name : getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()))
        if (column_name == name)
            return true;

    if (const auto * func = node->as<ASTFunction>())
        if (func->arguments->children.size() == 1)
            return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(func->arguments->children.front(), metadata_snapshot);

    return false;
}

bool MergeTreeMetaBase::mayBenefitFromIndexForIn(
    const ASTPtr & left_in_operand, ContextPtr, const StorageMetadataPtr & metadata_snapshot) const
{
    /// Make sure that the left side of the IN operator contain part of the key.
    /// If there is a tuple on the left side of the IN operator, at least one item of the tuple
    ///  must be part of the key (probably wrapped by a chain of some acceptable functions).
    const auto * left_in_operand_tuple = left_in_operand->as<ASTFunction>();
    const auto & index_wrapper_factory = MergeTreeIndexFactory::instance();
    if (left_in_operand_tuple && left_in_operand_tuple->name == "tuple")
    {
        for (const auto & item : left_in_operand_tuple->arguments->children)
        {
            if (isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(item, metadata_snapshot))
                return true;
            for (const auto & index : metadata_snapshot->getSecondaryIndices())
                if (index_wrapper_factory.get(index)->mayBenefitFromIndexForIn(item))
                    return true;
            if (metadata_snapshot->selected_projection
                && metadata_snapshot->selected_projection->isPrimaryKeyColumnPossiblyWrappedInFunctions(item))
                return true;
        }
        /// The tuple itself may be part of the primary key, so check that as a last resort.
        if (isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(left_in_operand, metadata_snapshot))
            return true;
        if (metadata_snapshot->selected_projection
            && metadata_snapshot->selected_projection->isPrimaryKeyColumnPossiblyWrappedInFunctions(left_in_operand))
            return true;
        return false;
    }
    else
    {
        for (const auto & index : metadata_snapshot->getSecondaryIndices())
            if (index_wrapper_factory.get(index)->mayBenefitFromIndexForIn(left_in_operand))
                return true;

        if (metadata_snapshot->selected_projection
            && metadata_snapshot->selected_projection->isPrimaryKeyColumnPossiblyWrappedInFunctions(left_in_operand))
            return true;

        return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(left_in_operand, metadata_snapshot);
    }
}

TableDefinitionHash MergeTreeMetaBase::getTableHashForClusterBy() const
{
    auto metadata = getInMemoryMetadataPtr();
    const auto & partition_by_ast = metadata->getPartitionKeyAST();
    const auto & order_by_ast = metadata->getSortingKeyAST();
    const auto & cluster_by_ast = metadata->getClusterByKeyAST();
    String partition_by = partition_by_ast ? queryToString(partition_by_ast) : "";
    String order_by = order_by_ast ? queryToString(order_by_ast) : "";
    String cluster_by = cluster_by_ast ? queryToString(cluster_by_ast) : "";

    String cluster_definition = partition_by + order_by + cluster_by;

    cluster_definition.erase(remove(cluster_definition.begin(), cluster_definition.end(), '\''), cluster_definition.end());

    UInt64 determin_hash = sipHash64(cluster_definition);

    UInt64 v1_hash = compatibility::v1::hash(cluster_definition);
    UInt64 v2_hash = compatibility::v2::hash(cluster_definition);

    /// Get v1 quoted hash for backward compatibility
    String quoted_partition_by = partition_by_ast ? queryToString(partition_by_ast, true) : "";
    String quoted_order_by = order_by_ast ? queryToString(order_by_ast, true) : "";
    String quoted_cluster_by = cluster_by_ast ? queryToString(cluster_by_ast, true) : "";
    String quoted_cluster_definition = quoted_partition_by + quoted_order_by + quoted_cluster_by;
    quoted_cluster_definition.erase(remove(quoted_cluster_definition.begin(), quoted_cluster_definition.end(), '\''), quoted_cluster_definition.end());
    UInt64 v1_quoted_hash = compatibility::v1::hash(quoted_cluster_definition);

    return {determin_hash, v1_hash, v2_hash, v1_quoted_hash};

}

bool MergeTreeMetaBase::isTableClustered(ContextPtr context_) const
{
    bool clustered;
    context_->getCnchCatalog()->getTableClusterStatus(getStorageUUID(), clustered);
    return clustered;
}

StorageSnapshotPtr MergeTreeMetaBase::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr  /*query_context*/) const
{
    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, object_columns);
}

StorageSnapshotPtr MergeTreeMetaBase::getStorageSnapshotWithoutParts(const StorageMetadataPtr & metadata_snapshot) const
{
    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, object_columns);
}

MergeTreeSettingsPtr MergeTreeMetaBase::getChangedSettings(const ASTPtr new_settings) const
{
    MergeTreeSettingsPtr changed_settings = getSettings();
    if (new_settings)
    {
        const auto & new_changes = new_settings->as<const ASTSetQuery &>().changes;
        auto copy = getDefaultSettings();
        copy->applyChanges(new_changes);
        changed_settings = std::move(copy);
    }

    return changed_settings;
}

void MergeTreeMetaBase::checkMetadataValidity(const ColumnsDescription & columns, const ASTPtr & new_settings) const
{
    NamesAndTypesList func_columns = getInMemoryMetadataPtr()->getFuncColumns();
    MergeTreeSettingsPtr current_settings = getChangedSettings(new_settings);

    if (current_settings->enable_unique_partial_update && !current_settings->partition_level_unique_keys)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not support table level unique keys when enable unique partial update.");

    auto columns_physical = columns.getAllPhysical();
    for (auto & column: columns_physical)
    {
        /// Check func columns
        for (auto & [name, type]: func_columns)
        {
            if (name == column.name)
                throw Exception("Column " + backQuoteIfNeed(column.name) + " is reserved column", ErrorCodes::BAD_ARGUMENTS);
        }

        /// block implicit key name for MergeTree family
        if (isMapImplicitKey(column.name))
            throw Exception("Column " + backQuoteIfNeed(column.name) + " contains reserved prefix word", ErrorCodes::BAD_ARGUMENTS);
        
        /// Block implicit key name for MergeTree family
        if (column.type && column.type->isMap())
        {
            const auto & type_map = typeid_cast<const DataTypeMap &>(*column.type);
            /// Check map validity
            type_map.checkValidity();

            /// Check constraint for KVMap
            if (column.type->isKVMap())
            {
                // To facilitate the processing of file formats compatible with community map and ByteKV map, we restrict the column names of KV map.
                for (const auto & key : MAP_KV_RESERVED_KEYS)
                {
                    if (column.name.find(key) != String::npos)
                        throw Exception(
                            "Column " + backQuoteIfNeed(column.name) + " contains reserved prefix word " + key, ErrorCodes::BAD_ARGUMENTS);
                }
            }
            /// Check constraint for ByteMap
            else
            {
                /// To compatible with old table which may have a Map(xx, Nullable(xx)) type, we can't check this in DataTypeMap
                /// DataTypeLowCardinality->isNullable is false
                if (type_map.getValueType()->isNullable())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "ByteMap column in table cannot have nullable value type, but column {} has type {}",
                        backQuoteIfNeed(column.name),
                        type_map.getName());

                auto escape_name = escapeForFileName(column.name + getMapSeparator());
                auto pos = escape_name.find(getMapSeparator());
                /// The name of map column should not contain map separator, which is convenient for extracting map column name from a implicit column name.
                if (pos + getMapSeparator().size() != escape_name.size())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Map column name {} is invalid because its escaped name {} contains reserved prefix word {} of map implicit column",
                        backQuoteIfNeed(column.name),
                        backQuoteIfNeed(escape_name),
                        getMapSeparator());
            }
        }
        else
        {
            /// Column names of non map types need to meet the following constraints, otherwise data will be written to the same file, resulting in errors.
            /// For example: `col.key` UInt64, `col` Map<String, UInt64> KV, these two columns will both write data to file col%2Ekey.bin.
            auto map_col = std::find_if(columns_physical.begin(), columns_physical.end(), [&](auto & col) {
                if (col.type->isKVMap())
                {
                    for (const auto & key : MAP_KV_RESERVED_KEYS)
                    {
                        if (startsWith(column.name, col.name + key))
                            return true;
                    }
                }
                return false;
            });
            if (map_col != columns_physical.end())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The name of column {} is not compatible with that of KV map column {}",
                    backQuoteIfNeed(column.name),
                    backQuoteIfNeed(map_col->name));
        }

        // _row_exists is reserved for DELETE mutation.
        if (column.name == RowExistsColumn::ROW_EXISTS_COLUMN.name)
            throw Exception("Column name " + backQuoteIfNeed(column.name) + " is reserved for DELETE mutation.", ErrorCodes::ILLEGAL_COLUMN);
    }
}

void MergeTreeMetaBase::checkTypeInComplianceWithRecommendedUsage(const DataTypePtr & type)
{
    /// Check kv flags if contains nested map
    if (!(type->getFlags() & TYPE_MAP_KV_STORE_FLAG) && type->hasNestedMap())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Data type {} contains nested map type but KV flags is not set, use \"{} KV\" instead.",
            type->getName(), type->getName());
}

bool MergeTreeMetaBase::commitTxnInWriteSuffixStage(const UInt32 & deup_impl_version, ContextPtr query_context) const
{
    if (!getInMemoryMetadataPtr()->hasUniqueKey() || static_cast<DedupImplVersion>(deup_impl_version) == DedupImplVersion::DEDUP_IN_TXN_COMMIT)
        return false;
    bool enable_staging_area = query_context->getSettingsRef().enable_staging_area_for_write || getSettings()->cloud_enable_staging_area;
    bool enable_append_mode = query_context->getSettingsRef().dedup_key_mode == DedupKeyMode::APPEND;
    return !enable_append_mode && !enable_staging_area;
}

bool MergeTreeMetaBase::supportsWriteInWorkers(const Context & query_context) const
{
    if (!getInMemoryMetadataPtr()->hasUniqueKey())
        return true;
    const auto & query_settings = query_context.getSettingsRef();
    const auto & table_settings = getSettings();
    if (query_settings.optimize_unique_table_write)
    {
        if (table_settings->dedup_impl_version.value == DedupImplVersion::DEDUP_IN_TXN_COMMIT)
            return true;
        LOG_DEBUG(log, "Can not write in workers due to dedup impl version is {}", table_settings->dedup_impl_version.value);
    }
    bool enable_staging_area = query_context.getSettingsRef().enable_staging_area_for_write || getSettings()->cloud_enable_staging_area;
    bool enable_append_mode = query_context.getSettingsRef().dedup_key_mode == DedupKeyMode::APPEND;
    return enable_append_mode || enable_staging_area;
}


ColumnSize MergeTreeMetaBase::getMapColumnSizes(const DataPartPtr & part, const String & map_implicit_column_name) const
{
    auto part_checksums = part->getChecksums();
    if (part_checksums->empty())
        return {};

    // we don't modify anything, maybe we do not need this lock
    auto lock = lockPartsRead();

    // __string_params__'btm' -> string_params
    auto name = parseMapNameFromImplicitColName(map_implicit_column_name) ;
    auto pair = part->columns_ptr->tryGetByName(name);
    if (pair.has_value())
    {
        auto part_column_size = part->getMapColumnSize(map_implicit_column_name, *(pair->type));
        return part_column_size;
    }
    return {};
}

ColumnSize MergeTreeMetaBase::calculateMapColumnSizesImpl(const String & map_implicit_column_name) const
{
    Stopwatch stopwatch;
    auto parts_and_ratio = getCalculatedPartsAndRatio();
    DataPartsVector calculated_parts = parts_and_ratio.first;
    double ratio = parts_and_ratio.second;

    ColumnSize map_column_size;
    for (const auto & part : calculated_parts)
    {
        const ColumnSize & map_col_size = getMapColumnSizes(part, map_implicit_column_name);
        map_column_size.add(map_col_size);
    }

    if (ratio > 1.0)
    {
        map_column_size.marks = map_column_size.marks * ratio;
        map_column_size.data_compressed = map_column_size.data_compressed * ratio;
        map_column_size.data_uncompressed = map_column_size.data_uncompressed * ratio;
    }

    LOG_DEBUG(log, "Calculate columns size elapsed: {} ms.", stopwatch.elapsedMilliseconds());

    return map_column_size;
}

ASTPtr MergeTreeMetaBase::applyFilter(
    ASTPtr query_filter, SelectQueryInfo & query_info, ContextPtr query_context, PlanNodeStatisticsPtr storage_statistics) const
{
    const auto & settings = query_context->getSettingsRef();
    auto * select_query = query_info.getSelectQuery();
    ASTs conjuncts = PredicateUtils::extractConjuncts(query_filter);

    /// Set partition_filter
    /// this should be done before setting query.where() to avoid partition filters being chosen as prewhere
    if (settings.enable_partition_filter_push_down)
    {
        ASTs push_predicates;
        ASTs remain_predicates;

        Names partition_key_names = getInMemoryMetadataPtr()->getPartitionKey().column_names;
        Names virtual_key_names = getSampleBlockWithVirtualColumns().getNames();
        partition_key_names.insert(partition_key_names.end(), virtual_key_names.begin(), virtual_key_names.end());
        auto iter = std::stable_partition(conjuncts.begin(), conjuncts.end(), [&](const auto & predicate) {
            PartitionPredicateVisitor::Data visitor_data{
                query_context, (const_cast<MergeTreeMetaBase *>(this))->shared_from_this(), predicate};
            PartitionPredicateVisitor(visitor_data).visit(predicate);
            return visitor_data.getMatch();
        });

        push_predicates.insert(push_predicates.end(), conjuncts.begin(), iter);
        remain_predicates.insert(remain_predicates.end(), iter, conjuncts.end());

        ASTPtr new_partition_filter;

        if (query_info.partition_filter)
        {
            push_predicates.push_back(query_info.partition_filter);
            new_partition_filter = PredicateUtils::combineConjuncts(push_predicates);
        }
        else
        {
            new_partition_filter = PredicateUtils::combineConjuncts<false>(push_predicates);
        }

        if (!PredicateUtils::isTruePredicate(new_partition_filter))
            query_info.partition_filter = std::move(new_partition_filter);

        conjuncts.swap(remain_predicates);
    }

    /// Set query.where()
    IStorage::applyFilter(PredicateUtils::combineConjuncts(conjuncts), query_info, query_context, storage_statistics);

    /// Set query.prewhere(), strategy 1: by selectivity
    if (select_query->where() && !select_query->prewhere() && supportsPrewhere() && settings.enable_active_prewhere && storage_statistics)
    {
        std::vector<ASTPtr> full_conjuncts = PredicateUtils::extractConjuncts(select_query->getWhere());
        std::vector<ASTPtr> pre_conjuncts;
        std::vector<ASTPtr> where_conjuncts;

        IdentifierNameSet used_columns;
        select_query->getWhere()->collectIdentifierNames(used_columns);
        const auto & columns_desc = getInMemoryMetadataPtr()->getColumns();
        NamesAndTypes names_and_types;
        for (const auto & col_name : used_columns)
            names_and_types.emplace_back(columns_desc.getPhysical(col_name));

        for (const auto & conjunct : full_conjuncts)
        {
            double selectivity = FilterEstimator::estimateFilterSelectivity(storage_statistics, conjunct, names_and_types, query_context);
            LOG_DEBUG(
                ::getLogger("OptimizerActivePrewhere"),
                "conjunct=" + serializeAST(*conjunct) + ", selectivity=" + std::to_string(selectivity));

            if (selectivity <= query_context->getSettingsRef().max_active_prewhere_selectivity
                && pre_conjuncts.size() < query_context->getSettingsRef().max_active_prewhere_size)
                pre_conjuncts.push_back(conjunct);
            else
                where_conjuncts.push_back(conjunct);
        }

        if (!pre_conjuncts.empty())
            select_query->setExpression(ASTSelectQuery::Expression::PREWHERE, PredicateUtils::combineConjuncts(pre_conjuncts));

        if (!where_conjuncts.empty())
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineConjuncts(where_conjuncts));
        else
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
    }

    /// Set query.prewhere(), strategy 2: by IO cost
    if (select_query->where() && !select_query->prewhere() && supportsPrewhere() && settings.enable_optimizer_early_prewhere_push_down)
    {
        /// PREWHERE optimization: transfer some condition from WHERE to PREWHERE if enabled and viable
        if (const auto & column_size = getColumnSizes(); !column_size.empty())
        {
            /// Extract column compressed sizes.
            std::unordered_map<std::string, UInt64> column_compressed_sizes;
            for (const auto & [name, sizes] : column_size)
                column_compressed_sizes[name] = sizes.data_compressed;

            auto current_info = buildSelectQueryInfoForQuery(query_info.query, query_context);

            for (const auto & column_name : current_info.syntax_analyzer_result->requiredSourceColumns())
            {
                UInt64 size = getColumnCompressedSize(column_name);
                // Now get implicit column size only for prewhere pushdown
                if (size == 0 && query_context->getSettingsRef().enable_implicit_column_prewhere_push && isMapImplicitKey(column_name))
                {
                    if (auto cloud_merge_tree = dynamic_cast<const StorageCloudMergeTree *>(this))
                        size = cloud_merge_tree->calculateMapColumnSizesImpl(column_name).data_compressed;
                }
                column_compressed_sizes[column_name] = size;
            }

            MergeTreeWhereOptimizer{
                current_info,
                query_context,
                std::move(column_compressed_sizes),
                getInMemoryMetadataPtr(),
                current_info.syntax_analyzer_result->requiredSourceColumns(),
                ::getLogger("OptimizerEarlyPrewherePushdown")};
        }
    }

    /// remove prewhere from query plan
    if (auto prewhere = select_query->prewhere())
        PredicateUtils::subtract(conjuncts, PredicateUtils::extractConjuncts(prewhere));

    return PredicateUtils::combineConjuncts(conjuncts);
}

bool MergeTreeMetaBase::canFilterPartitionByTTL() const
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    TTLTableDescription table_ttl = metadata_snapshot->getTableTTLs();
    return metadata_snapshot->hasPartitionLevelTTL() && table_ttl.definition_ast;
}

void MergeTreeMetaBase::filterPartitionByTTL(std::vector<std::shared_ptr<MergeTreePartition>> & partition_list, time_t query_time) const
{
    if (canFilterPartitionByTTL())
    {
        const auto & metadata_snapshot = getInMemoryMetadataPtr();
        if (!metadata_snapshot->hasRowsTTL())
            return;

        /// make a copy of rows_ttl, we may rewrite it later.
        auto rows_ttl = metadata_snapshot->table_ttl.rows_ttl;

        /// Construct a block consists of partition keys then compute ttl values according to this block
        const auto & partition_key_sample = metadata_snapshot->getPartitionKey().sample_block;
        MutableColumns columns = partition_key_sample.cloneEmptyColumns();

        for (const auto & partition : partition_list)
        {
            /// This can happen when ALTER query is implemented improperly; finish ALTER query should bypass this check.
            if (columns.size() != partition->value.size())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Partition key columns definition missmatch between inmemory and metastore, this is a bug, expect block ({}), got values "
                    "({})\n",
                    partition_key_sample.dumpNames(),
                    fmt::join(partition->value, ", "));

            for (size_t i = 0; i < partition->value.size(); ++i)
                columns[i]->insert(partition->value[i]);
        }

        auto block = partition_key_sample.cloneWithColumns(std::move(columns));
        TTLDescription::tryRewriteTTLWithPartitionKey(rows_ttl, metadata_snapshot->columns, metadata_snapshot->partition_key, metadata_snapshot->primary_key, getContext(), allow_nullable_key);
        rows_ttl.expression->execute(block);

        // got the ttl values for each partition based on ttl expression
        const auto & ttl_values = block.getByName(rows_ttl.result_column);
        const IColumn * column = ttl_values.column.get();

        if (column->size() != partition_list.size())
            throw Exception("Calculated TTL column size cannot match input partitions column size.", ErrorCodes::LOGICAL_ERROR);

        if (query_time == 0)
            query_time = std::time(nullptr);

        std::vector<std::shared_ptr<MergeTreePartition>> filtered_result;

        if (column->isNullable())
            column = static_cast<const ColumnNullable *>(column)->getNestedColumnPtr().get();

        if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(column))
        {
            const auto & date_lut = DateLUT::serverTimezoneInstance();
            for (size_t index = 0; index < column->size(); index++)
            {
                auto ttl_value = date_lut.fromDayNum(DayNum(column_date->getElement(index)));
                if (ttl_value >= query_time)
                    filtered_result.push_back(partition_list[index]);
            }
        }
        else if (const ColumnUInt32 * column_date_time = typeid_cast<const ColumnUInt32 *>(column))
        {
            for (size_t index = 0; index < column->size(); index++)
            {
                auto ttl_value = column_date_time->getElement(index);
                if (ttl_value >= query_time)
                    filtered_result.push_back(partition_list[index]);
            }
        }
        else
            throw Exception("Unexpected type of result ttl column", ErrorCodes::LOGICAL_ERROR);

        // for (size_t index = 0; index < column->size(); index++)
        // {
        //     time_t ttl_value = 0;
        //     if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(column))
        //     {
        //         const auto & date_lut = DateLUT::serverTimezoneInstance();
        //         ttl_value = date_lut.fromDayNum(DayNum(column_date->getElement(index)));
        //     }
        //     else if (const ColumnUInt32 * column_date_time = typeid_cast<const ColumnUInt32 *>(column))
        //     {
        //         ttl_value = column_date_time->getElement(index);
        //     }
        //     else
        //         throw Exception("Unexpected type of result ttl column", ErrorCodes::LOGICAL_ERROR);

        //     if (ttl_value >= query_time)
        //         filtered_result.push_back(partition_list[index]);
        // }

        if (filtered_result.size() < partition_list.size())
            LOG_DEBUG(log, "TTL rules dropped {} partitions (before: {}).", partition_list.size() - filtered_result.size(), partition_list.size());
        filtered_result.swap(partition_list);
    }
}

Strings MergeTreeMetaBase::selectPartitionsByPredicate(
    const SelectQueryInfo & query_info,
    std::vector<std::shared_ptr<MergeTreePartition>> & partition_list,
    const Names & /* column_names_to_return */,
    ContextPtr local_context,
    const bool & ignore_ttl) const
{
    // LOG_TRACE(
    //     log,
    //     "selectPartitionsByPredicate, query: {}, partition_filter: {}, partition size before pruning: {}",
    //     query_info.query->formatForErrorMessage(),
    //     query_info.partition_filter ? query_info.partition_filter->formatForErrorMessage() : "NULL",
    //     partition_list.size());

    /// Coarse grained partition pruner: filter out the partition which will definitely not satisfy the query predicate. The benefit
    /// is 2-folded: (1) we can prune data parts and (2) we can reduce numbers of calls to catalog to get parts 's metadata.
    /// Note that this step still leaves false-positive parts. For example, the partition key is `toMonth(date)` and the query
    /// condition is `date > '2022-02-22' and date < '2022-03-22'` then this step won't eliminate any partition.

    /// The partition pruning rules come from 3 types:
    /// (1) TTL
    /// (2) Columns in predicate that exactly match the partition key
    /// (3) `_partition_id` or `_partition_value` if they're in predicate

    /// (1) Prune partition by partition level TTL
    if (!ignore_ttl)
        filterPartitionByTTL(partition_list, local_context->tryGetCurrentTransactionID().toSecond());

    const auto partition_key = MergeTreePartition::adjustPartitionKey(getInMemoryMetadataPtr(), local_context);
    const auto & partition_key_expr = partition_key.expression;
    const auto & partition_key_sample = partition_key.sample_block;
    if (partition_key_sample.columns() > 0)
    {
        /// (2) Prune partitions if there's a column in predicate that exactly match the partition key
        Names partition_key_columns;
        for (const auto & name : partition_key_sample)
        {
            partition_key_columns.emplace_back(name.name);
        }

        KeyCondition partition_condition(query_info, local_context, partition_key_columns, partition_key_expr);
        // LOG_TRACE(log, "partition_condition: {}", partition_condition.toString());
        DataTypes result;
        result.reserve(partition_key_sample.getDataTypes().size());
        for (const auto & data_type : partition_key_sample.getDataTypes())
        {
            result.push_back(DataTypeFactory::instance().get(data_type->getName(), data_type->getFlags()));
        }
        size_t prev_sz = partition_list.size();
        std::erase_if(partition_list, [&](const auto & partition) {
            const auto & partition_value = partition->value;
            std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
            auto res = partition_condition.mayBeTrueInRange(partition_key_columns.size(), index_value.data(), index_value.data(), result);
            LOG_TRACE(
                log,
                "Key condition {} is {} in [ ({}) - ({}) )",
                partition_condition.toString(),
                res,
                fmt::join(index_value, " "),
                fmt::join(index_value, " "));
            return !res;
        });
        if (partition_list.size() < prev_sz)
            LOG_DEBUG(log, "Query predicates on physical columns dropped {} partitions", prev_sz - partition_list.size());

        if (!partition_list.empty())
        {
            Block partition_block = getPartitionBlockWithVirtualColumns(partition_list);
            ASTPtr expression_ast;

            /// Generate valid expressions for filtering
            VirtualColumnUtils::prepareFilterBlockWithQuery(
                query_info.query, local_context, partition_block, expression_ast, query_info.partition_filter);

            // LOG_TRACE(
            //     log,
            //     "prepared filter {} is prepared, by block {}",
            //     expression_ast ? expression_ast->formatForErrorMessage() : "NULL",
            //     partition_block.dumpStructure());

            /// Generate list of partition id that fit the query predicate
            NameSet partition_ids;
            if (expression_ast && !KeyDescription::moduloToModuloLegacyRecursive(expression_ast->clone()))
            {
                replace_func_with_known_column(expression_ast, NameSet{partition_key_columns.begin(), partition_key_columns.end()});
                VirtualColumnUtils::filterBlockWithQuery(
                    query_info.query, partition_block, local_context, expression_ast, query_info.partition_filter);
                partition_ids = VirtualColumnUtils::extractSingleValueFromBlock<String>(partition_block, "_partition_id");
                /// Prunning
                prev_sz = partition_list.size();
                std::erase_if(partition_list, [this, &partition_ids](const auto & partition) {
                    return partition_ids.find(partition->getID(*this)) == partition_ids.end();
                });
                if (partition_list.size() < prev_sz)
                    LOG_DEBUG(
                        log,
                        "Query predicates on `_partition_id` and `_partition_value` droped {} partitions",
                        prev_sz - partition_list.size());
            }
        }
    }
    Strings res_partitions;
    for (const auto & partition : partition_list)
        res_partitions.emplace_back(partition->getID(*this));

    return res_partitions;
}

void MergeTreeMetaBase::getDeleteBitmapMetaForServerParts(const ServerDataPartsVector & parts, DeleteBitmapMetaPtrVector & all_bitmaps, bool force_found) const
{
    DeleteBitmapMetaPtrVector bitmaps;
    CnchPartsHelper::calcVisibleDeleteBitmaps(all_bitmaps, bitmaps);

    /// Both the parts and bitmaps are sorted in (partition_id, min_block, max_block, commit_time) order
    auto bitmap_it = bitmaps.begin();
    for (const auto & part : parts)
    {
        if (force_found)
        {
            /// search for the first bitmap
            while (bitmap_it != bitmaps.end() && !(*bitmap_it)->sameBlock(part->info()))
                bitmap_it++;

            if (bitmap_it == bitmaps.end())
            {
                if (auto unique_table_log = getContext()->getCloudUniqueTableLog())
                {
                    auto current_log = UniqueTable::createUniqueTableLog(UniqueTableLogElement::ERROR, getCnchStorageID());
                    current_log.metric = ErrorCodes::LOGICAL_ERROR;
                    current_log.event_msg = "Delete bitmap metadata of " + part->name() + " is not found";
                    unique_table_log->add(current_log);
                }
                throw Exception("Delete bitmap metadata of " + part->name() + " is not found", ErrorCodes::LOGICAL_ERROR);
            }

            /// add all visible bitmaps (from new to old) part
            bool found_base = false;
            auto list_it = part->delete_bitmap_metas.before_begin();
            for (auto bitmap_meta = *bitmap_it; bitmap_meta; bitmap_meta = bitmap_meta->tryGetPrevious())
            {
                list_it = part->delete_bitmap_metas.insert_after(list_it, bitmap_meta->getModel());
                if (bitmap_meta->getType() == DeleteBitmapMetaType::Base)
                {
                    found_base = true;
                    break;
                }
            }
            if (!found_base)
            {
                if (auto unique_table_log = getContext()->getCloudUniqueTableLog())
                {
                    auto current_log = UniqueTable::createUniqueTableLog(UniqueTableLogElement::ERROR, getCnchStorageID());
                    current_log.metric = ErrorCodes::LOGICAL_ERROR;
                    current_log.event_msg = "Base delete bitmap of " + part->name() + " is not found";
                    unique_table_log->add(current_log);
                }
                throw Exception("Base delete bitmap of " + part->name() + " is not found", ErrorCodes::LOGICAL_ERROR);
            }

            bitmap_it++;
        }
        else
        {
            while (bitmap_it != bitmaps.end() && (*(*bitmap_it)) <= part->info())
            {
                if (!(*bitmap_it)->sameBlock(part->info()))
                    bitmap_it++;
                else
                {
                    /// add all visible bitmaps (from new to old) part
                    bool found_base = false;
                    auto list_it = part->delete_bitmap_metas.before_begin();
                    for (auto bitmap_meta = *bitmap_it; bitmap_meta; bitmap_meta = bitmap_meta->tryGetPrevious())
                    {
                        list_it = part->delete_bitmap_metas.insert_after(list_it, bitmap_meta->getModel());
                        if (bitmap_meta->getType() == DeleteBitmapMetaType::Base)
                        {
                            found_base = true;
                            break;
                        }
                    }
                    if (!found_base)
                    {
                        if (auto unique_table_log = getContext()->getCloudUniqueTableLog())
                        {
                            auto current_log = UniqueTable::createUniqueTableLog(UniqueTableLogElement::ERROR, getCnchStorageID());
                            current_log.metric = ErrorCodes::LOGICAL_ERROR;
                            current_log.event_msg = "Base delete bitmap of " + part->name() + " is not found";
                            unique_table_log->add(current_log);
                        }
                        throw Exception("Base delete bitmap of " + part->name() + " is not found", ErrorCodes::LOGICAL_ERROR);
                    }
                    bitmap_it++;
                }
            }
        }
    }
}

void MergeTreeMetaBase::getDeleteBitmapMetaForCnchParts(MutableMergeTreeDataPartsCNCHVector & parts, DeleteBitmapMetaPtrVector & all_bitmaps, bool force_found)
{
    MergeTreeDataPartsCNCHVector cnch_parts;
    cnch_parts.reserve(parts.size());
    for (auto & part : parts)
        cnch_parts.emplace_back(const_pointer_cast<const MergeTreeDataPartCNCH>(part));
    getDeleteBitmapMetaForCnchParts(cnch_parts, all_bitmaps, force_found);
}

void MergeTreeMetaBase::getDeleteBitmapMetaForCnchParts(const MergeTreeDataPartsCNCHVector & parts, DeleteBitmapMetaPtrVector & all_bitmaps, bool force_found)
{
    DeleteBitmapMetaPtrVector bitmaps;
    CnchPartsHelper::calcVisibleDeleteBitmaps(all_bitmaps, bitmaps);

    /// Both the parts and bitmaps are sorted in (partition_id, min_block, max_block, commit_time) order
    auto bitmap_it = bitmaps.begin();
    for (auto & part : parts)
    {
        if (force_found)
        {
            /// search for the first bitmap
            while (bitmap_it != bitmaps.end() && !(*bitmap_it)->sameBlock(part->info))
                bitmap_it++;

            if (bitmap_it == bitmaps.end())
            {
                if (auto unique_table_log = getContext()->getCloudUniqueTableLog())
                {
                    auto current_log = UniqueTable::createUniqueTableLog(UniqueTableLogElement::ERROR, getCnchStorageID());
                    current_log.metric = ErrorCodes::LOGICAL_ERROR;
                    current_log.event_msg = "Delete bitmap metadata of " + part->name + " is not found";
                    unique_table_log->add(current_log);
                }
                throw Exception("Delete bitmap metadata of " + part->name + " is not found", ErrorCodes::LOGICAL_ERROR);
            }

            /// add all visible bitmaps (from new to old) part
            part->setDeleteBitmapMeta(*bitmap_it);
            bitmap_it++;
        }
        else
        {
            while (bitmap_it != bitmaps.end() && (*(*bitmap_it)) <= part->info)
            {
                if (!(*bitmap_it)->sameBlock(part->info))
                    bitmap_it++;
                else
                {
                    /// add all visible bitmaps (from new to old) part
                    part->setDeleteBitmapMeta(*bitmap_it, /*force_set*/ false);
                    bitmap_it++;
                }
            }
        }
    }
}

void MergeTreeMetaBase::getDeleteBitmapMetaForParts(IMergeTreeDataPartsVector & parts, DeleteBitmapMetaPtrVector & delete_bitmap_metas, bool force_found)
{
    MergeTreeDataPartsCNCHVector cnch_parts;
    cnch_parts.reserve(parts.size());
    for (auto & part : parts)
        cnch_parts.emplace_back(dynamic_pointer_cast<const MergeTreeDataPartCNCH>(part));
    getDeleteBitmapMetaForCnchParts(cnch_parts, delete_bitmap_metas, force_found);
}

void MergeTreeMetaBase::getDeleteBitmapMetaForStagedParts(
    const MergeTreeDataPartsCNCHVector & parts, ContextPtr local_context, TxnTimestamp start_time)
{
    auto catalog = local_context->getCnchCatalog();
    if (!catalog)
        return;

    std::set<String> request_partitions;
    std::set<int64_t> request_buckets;
    for (const auto & part : parts)
    {
        const auto & partition_id = part->info.partition_id;
        request_partitions.insert(partition_id);
        request_buckets.insert(part->bucket_number);
    }

    /// NOTE: Get all the bitmap meta needed only once from kv instead of getting many times for every partition to save time.
    Stopwatch watch;
    auto all_bitmaps = catalog->getDeleteBitmapsInPartitions(
        shared_from_this(),
        {request_partitions.begin(), request_partitions.end()},
        start_time,
        nullptr,
        Catalog::VisibilityLevel::Visible,
        request_buckets);
    ProfileEvents::increment(ProfileEvents::CatalogTime, watch.elapsedMilliseconds());
    LOG_DEBUG(
        log,
        "Get delete bitmap meta for total {} parts, take {} ms and read {} number of bitmap metas",
        parts.size(),
        watch.elapsedMilliseconds(),
        all_bitmaps.size());

    DeleteBitmapMetaPtrVector bitmaps;
    CnchPartsHelper::calcVisibleDeleteBitmaps(all_bitmaps, bitmaps);

    /// Both the parts and bitmaps are sorted in (partitioin_id, min_block, max_block, commit_time) order
    auto bitmap_it = bitmaps.begin();
    for (const auto & part : parts)
    {
        while (bitmap_it != bitmaps.end() && (*(*bitmap_it)) <= part->info)
        {
            if (!(*bitmap_it)->sameBlock(part->info))
                bitmap_it++;
            else
            {
                /// add all visible bitmaps (from new to old) part
                part->setDeleteBitmapMeta(*bitmap_it);
                bitmap_it++;
            }
        }
    }
}

}
