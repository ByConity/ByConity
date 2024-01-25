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

#include <Storages/StorageInMemoryMetadata.h>

#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/MapHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTClusterByElement.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeEnum.h>
#include <Storages/ColumnsDescription.h>

#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>
#include <Common/typeid_cast.h>
#include <Storages/ForeignKeysDescription.h>
#include <Storages/UniqueNotEnforcedDescription.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
    extern const int DUPLICATE_COLUMN;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int TYPE_MISMATCH;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}

StorageInMemoryMetadata::StorageInMemoryMetadata(const StorageInMemoryMetadata & other)
    : columns(other.columns)
    , secondary_indices(other.secondary_indices)
    , constraints(other.constraints)
    , foreign_keys(other.foreign_keys)
    , unique_not_enforced(other.unique_not_enforced)
    , projections(other.projections.clone())
    , partition_key(other.partition_key)
    , cluster_by_key(other.cluster_by_key)
    , primary_key(other.primary_key)
    , sorting_key(other.sorting_key)
    , sampling_key(other.sampling_key)
    , unique_key(other.unique_key)
    , column_ttls_by_name(other.column_ttls_by_name)
    , table_ttl(other.table_ttl)
    , settings_changes(other.settings_changes ? other.settings_changes->clone() : nullptr)
    , select(other.select)
    , comment(other.comment)
{
}

StorageInMemoryMetadata & StorageInMemoryMetadata::operator=(const StorageInMemoryMetadata & other)
{
    if (&other == this)
        return *this;

    columns = other.columns;
    secondary_indices = other.secondary_indices;
    constraints = other.constraints;
    foreign_keys = other.foreign_keys;
    unique_not_enforced = other.unique_not_enforced;
    projections = other.projections.clone();
    partition_key = other.partition_key;
    cluster_by_key = other.cluster_by_key;
    primary_key = other.primary_key;
    sorting_key = other.sorting_key;
    unique_key = other.unique_key;
    sampling_key = other.sampling_key;
    column_ttls_by_name = other.column_ttls_by_name;
    table_ttl = other.table_ttl;
    if (other.settings_changes)
        settings_changes = other.settings_changes->clone();
    else
        settings_changes.reset();
    select = other.select;
    comment = other.comment;
    return *this;
}

void StorageInMemoryMetadata::setComment(const String & comment_)
{
    comment = comment_;
}

void StorageInMemoryMetadata::setColumns(ColumnsDescription columns_)
{
    if (columns_.getAllPhysical().empty())
        throw Exception("Empty list of columns passed", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
    columns = std::move(columns_);
}

void StorageInMemoryMetadata::setSecondaryIndices(IndicesDescription secondary_indices_)
{
    secondary_indices = std::move(secondary_indices_);
}

void StorageInMemoryMetadata::setConstraints(ConstraintsDescription constraints_)
{
    constraints = std::move(constraints_);
}

void StorageInMemoryMetadata::setForeignKeys(ForeignKeysDescription foreign_keys_)
{
    foreign_keys = std::move(foreign_keys_);
}

void StorageInMemoryMetadata::setUniqueNotEnforced(UniqueNotEnforcedDescription unique_)
{
    unique_not_enforced = std::move(unique_);
}

void StorageInMemoryMetadata::setProjections(ProjectionsDescription projections_)
{
    projections = std::move(projections_);
}

void StorageInMemoryMetadata::setTableTTLs(const TTLTableDescription & table_ttl_)
{
    table_ttl = table_ttl_;
}

void StorageInMemoryMetadata::setColumnTTLs(const TTLColumnsDescription & column_ttls_by_name_)
{
    column_ttls_by_name = column_ttls_by_name_;
}

void StorageInMemoryMetadata::setSettingsChanges(const ASTPtr & settings_changes_)
{
    if (settings_changes_)
        settings_changes = settings_changes_;
    else
        settings_changes = nullptr;
}

void StorageInMemoryMetadata::setSelectQuery(const SelectQueryDescription & select_)
{
    select = select_;
}

const ColumnsDescription & StorageInMemoryMetadata::getColumns() const
{
    return columns;
}

const IndicesDescription & StorageInMemoryMetadata::getSecondaryIndices() const
{
    return secondary_indices;
}

bool StorageInMemoryMetadata::hasSecondaryIndices() const
{
    return !secondary_indices.empty();
}

const ConstraintsDescription & StorageInMemoryMetadata::getConstraints() const
{
    return constraints;
}

const ForeignKeysDescription & StorageInMemoryMetadata::getForeignKeys() const
{
    return foreign_keys;
}

const UniqueNotEnforcedDescription & StorageInMemoryMetadata::getUniqueNotEnforced() const
{
    return unique_not_enforced;
}

const ProjectionsDescription & StorageInMemoryMetadata::getProjections() const
{
    return projections;
}

bool StorageInMemoryMetadata::hasProjections() const
{
    return !projections.empty();
}

TTLTableDescription StorageInMemoryMetadata::getTableTTLs() const
{
    return table_ttl;
}

bool StorageInMemoryMetadata::hasAnyTableTTL() const
{
    return hasAnyMoveTTL() || hasRowsTTL() || hasAnyRecompressionTTL() || hasAnyGroupByTTL() || hasAnyRowsWhereTTL();
}

TTLColumnsDescription StorageInMemoryMetadata::getColumnTTLs() const
{
    return column_ttls_by_name;
}

bool StorageInMemoryMetadata::hasAnyColumnTTL() const
{
    return !column_ttls_by_name.empty();
}

TTLDescription StorageInMemoryMetadata::getRowsTTL() const
{
    return table_ttl.rows_ttl;
}

bool StorageInMemoryMetadata::hasRowsTTL() const
{
    return table_ttl.rows_ttl.expression != nullptr;
}

bool StorageInMemoryMetadata::hasPartitionLevelTTL() const
{
    if (!hasRowsTTL())
        return false;

    NameSet partition_columns(partition_key.column_names.begin(), partition_key.column_names.end());

    std::function<bool(const ASTPtr &)> isInPartitions = [&](const ASTPtr expr) -> bool {
        String name = expr->getAliasOrColumnName();
        if (partition_columns.count(name))
            return true;

        if (auto literal = expr->as<ASTLiteral>())
            return true;
        if (auto identifier = expr->as<ASTIdentifier>())
            return false;
        if (auto func = expr->as<ASTFunction>())
        {
            bool res = true;
            for (auto & arg : func->arguments->children)
                res &= isInPartitions(arg);
            return res;
        }
        return false;
    };

    return isInPartitions(table_ttl.rows_ttl.expression_ast);
}

TTLDescriptions StorageInMemoryMetadata::getRowsWhereTTLs() const
{
    return table_ttl.rows_where_ttl;
}

bool StorageInMemoryMetadata::hasAnyRowsWhereTTL() const
{
    return !table_ttl.rows_where_ttl.empty();
}

TTLDescriptions StorageInMemoryMetadata::getMoveTTLs() const
{
    return table_ttl.move_ttl;
}

bool StorageInMemoryMetadata::hasAnyMoveTTL() const
{
    return !table_ttl.move_ttl.empty();
}

TTLDescriptions StorageInMemoryMetadata::getRecompressionTTLs() const
{
    return table_ttl.recompression_ttl;
}

bool StorageInMemoryMetadata::hasAnyRecompressionTTL() const
{
    return !table_ttl.recompression_ttl.empty();
}

TTLDescriptions StorageInMemoryMetadata::getGroupByTTLs() const
{
    return table_ttl.group_by_ttl;
}

bool StorageInMemoryMetadata::hasAnyGroupByTTL() const
{
    return !table_ttl.group_by_ttl.empty();
}

ColumnDependencies StorageInMemoryMetadata::getColumnDependencies(const NameSet & updated_columns) const
{
    if (updated_columns.empty())
        return {};

    ColumnDependencies res;

    NameSet indices_columns;
    NameSet projections_columns;
    NameSet required_ttl_columns;
    NameSet updated_ttl_columns;

    auto add_dependent_columns = [&updated_columns](const auto & expression, auto & to_set)
    {
        auto required_columns = expression->getRequiredColumns();
        for (const auto & dependency : required_columns)
        {
            if (updated_columns.count(dependency))
            {
                to_set.insert(required_columns.begin(), required_columns.end());
                return true;
            }
        }

        return false;
    };

    for (const auto & index : getSecondaryIndices())
        add_dependent_columns(index.expression, indices_columns);

    for (const auto & projection : getProjections())
        add_dependent_columns(&projection, projections_columns);

    if (hasRowsTTL())
    {
        auto rows_expression = getRowsTTL().expression;
        if (add_dependent_columns(rows_expression, required_ttl_columns))
        {
            /// Filter all columns, if rows TTL expression have to be recalculated.
            for (const auto & column : getColumns().getAllPhysical())
                updated_ttl_columns.insert(column.name);
        }
    }

    for (const auto & entry : getRecompressionTTLs())
        add_dependent_columns(entry.expression, required_ttl_columns);

    for (const auto & [name, entry] : getColumnTTLs())
    {
        if (add_dependent_columns(entry.expression, required_ttl_columns))
            updated_ttl_columns.insert(name);
    }

    for (const auto & entry : getMoveTTLs())
        add_dependent_columns(entry.expression, required_ttl_columns);

    for (const auto & column : indices_columns)
        res.emplace(column, ColumnDependency::SKIP_INDEX);
    for (const auto & column : projections_columns)
        res.emplace(column, ColumnDependency::PROJECTION);
    for (const auto & column : required_ttl_columns)
        res.emplace(column, ColumnDependency::TTL_EXPRESSION);
    for (const auto & column : updated_ttl_columns)
        res.emplace(column, ColumnDependency::TTL_TARGET);

    return res;

}

Block StorageInMemoryMetadata::getSampleBlockNonMaterialized(bool include_func_columns) const
{
    Block res;

    for (const auto & column : getColumns().getOrdinary())
        res.insert({column.type->createColumn(), column.type, column.name});

    if (include_func_columns)
    {
        for (const auto & column : getFuncColumns())
            res.insert({column.type->createColumn(), column.type, column.name});
    }

    return res;
}

Block StorageInMemoryMetadata::getSampleBlockWithVirtuals(const NamesAndTypesList & virtuals) const
{
    auto res = getSampleBlock();

    /// Virtual columns must be appended after ordinary, because user can
    /// override them.
    for (const auto & column : virtuals)
        res.insert({column.type->createColumn(), column.type, column.name});

    return res;
}

Block StorageInMemoryMetadata::getSampleBlock(bool include_func_columns) const
{
    Block res;

    for (const auto & column : getColumns().getAllPhysical())
        res.insert({column.type->createColumn(), column.type, column.name});

    if (include_func_columns)
    {
        for (const auto & column : getFuncColumns())
            res.insert({column.type->createColumn(), column.type, column.name});
    }

    return res;
}

Block StorageInMemoryMetadata::getSampleBlockWithDeleteFlag() const
{
    Block res = getSampleBlock();

    auto delete_flag_column = getFuncColumns().tryGetByName(DELETE_FLAG_COLUMN_NAME);
    if (delete_flag_column)
        res.insert({delete_flag_column->type->createColumn(), delete_flag_column->type, delete_flag_column->name});
    else
        throw Exception(
            ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
            "Column _delete_flag_ doesn't exist when executing method getSampleBlockWithDeleteFlag.");

    return res;
}

Block StorageInMemoryMetadata::getSampleBlockForColumns(
    const Names & column_names,
    const NamesAndTypesList & virtuals,
    const StorageID & storage_id,
    BitEngineReadType bitengine_read_type) const
{
    Block res;

    google::dense_hash_map<StringRef, const DataTypePtr *, StringRefHash> virtuals_map;
    virtuals_map.set_empty_key(StringRef());

    /// Virtual columns must be appended after ordinary, because user can
    /// override them.
    for (const auto & column : virtuals)
        virtuals_map.emplace(column.name, &column.type);

    for (const auto & name : column_names)
    {
        auto column = getColumns().tryGetColumnOrSubcolumn(GetColumnsOptions::All, name);
        if (column)
        {
            auto column_name = column->name;
            if (isBitmap64(column->type) && column->type->isBitEngineEncode()
                && bitengine_read_type == BitEngineReadType::ONLY_ENCODE)
                column_name += BITENGINE_COLUMN_EXTENSION;
            res.insert({column->type->createColumn(), column->type, column_name});
        }
        else if (auto it = virtuals_map.find(name); it != virtuals_map.end())
        {
            const auto & type = *it->second;
            res.insert({type->createColumn(), type, name});
        }
        else
            throw Exception(
                "Column " + backQuote(name) + " not found in table " + (storage_id.empty() ? "" : storage_id.getNameForLogs()),
                ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
    }

    return res;
}

const KeyDescription & StorageInMemoryMetadata::getPartitionKey() const
{
    return partition_key;
}

bool StorageInMemoryMetadata::isPartitionKeyDefined() const
{
    return partition_key.definition_ast != nullptr;
}

bool StorageInMemoryMetadata::hasPartitionKey() const
{
    return !partition_key.column_names.empty();
}

Names StorageInMemoryMetadata::getColumnsRequiredForPartitionKey() const
{
    if (hasPartitionKey())
        return partition_key.expression->getRequiredColumns();
    return {};
}

const KeyDescription & StorageInMemoryMetadata::getClusterByKey() const
{
    return cluster_by_key;
}

bool StorageInMemoryMetadata::isClusterByKeyDefined() const
{
    return cluster_by_key.definition_ast != nullptr;
}

bool StorageInMemoryMetadata::hasClusterByKey() const
{
    return !cluster_by_key.column_names.empty();
}

Names StorageInMemoryMetadata::getColumnsForClusterByKey() const
{
    if (hasClusterByKey())
        return cluster_by_key.column_names;
    return {};
}

Names StorageInMemoryMetadata::getColumnsRequiredForClusterByKey() const
{
    if (hasClusterByKey())
        return cluster_by_key.expression->getRequiredColumns();
    return {};
}

Int64 StorageInMemoryMetadata::getBucketNumberFromClusterByKey() const
{
    if (isClusterByKeyDefined())
        return cluster_by_key.definition_ast->as<ASTClusterByElement>()->getTotalBucketNumber()->as<ASTLiteral>()->value.get<Int64>();
    return -1;
}

Int64 StorageInMemoryMetadata::getSplitNumberFromClusterByKey() const
{
    if (hasClusterByKey())
        return cluster_by_key.definition_ast->as<ASTClusterByElement>()->split_number;
    return 0;
}

bool StorageInMemoryMetadata::getWithRangeFromClusterByKey() const
{
    if (hasClusterByKey())
        return cluster_by_key.definition_ast->as<ASTClusterByElement>()->is_with_range;
    return false;
}

bool StorageInMemoryMetadata::getIsUserDefinedExpressionFromClusterByKey() const
{
    if (hasClusterByKey())
        return cluster_by_key.definition_ast->as<ASTClusterByElement>()->is_user_defined_expression;
    return false;
}

bool StorageInMemoryMetadata::checkIfClusterByKeySameWithUniqueKey() const
{
    if (!hasUniqueKey())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only unique table can call method checkIfClusterByKeySameWithUniqueKey, but it's not unique table.");
    if (!isClusterByKeyDefined())
        return false;

    /// If required columns are same in unique key and cluster by expression, we can make sure that all same unique key will split into the same bucket while writing.
    Names unique_key_required_columns = getColumnsRequiredForUniqueKey();
    Names cluster_by_key_required_columns = getColumnsRequiredForClusterByKey();
    if (unique_key_required_columns.size() != cluster_by_key_required_columns.size())
        return false;
    NameSet unique_key_required_columns_set = {unique_key_required_columns.begin(), unique_key_required_columns.end()};
    for (const auto & column: cluster_by_key_required_columns)
    {
        if (!unique_key_required_columns_set.count(column))
            return false;
    }
    return true;
}

const KeyDescription & StorageInMemoryMetadata::getSortingKey() const
{
    return sorting_key;
}

bool StorageInMemoryMetadata::isSortingKeyDefined() const
{
    return sorting_key.definition_ast != nullptr;
}

bool StorageInMemoryMetadata::hasSortingKey() const
{
    return !sorting_key.column_names.empty();
}

Names StorageInMemoryMetadata::getColumnsRequiredForSortingKey() const
{
    if (hasSortingKey())
        return sorting_key.expression->getRequiredColumns();
    return {};
}

Names StorageInMemoryMetadata::getSortingKeyColumns() const
{
    if (hasSortingKey())
        return sorting_key.column_names;
    return {};
}

const KeyDescription & StorageInMemoryMetadata::getSamplingKey() const
{
    return sampling_key;
}

bool StorageInMemoryMetadata::isSamplingKeyDefined() const
{
    return sampling_key.definition_ast != nullptr;
}

bool StorageInMemoryMetadata::hasSamplingKey() const
{
    return !sampling_key.column_names.empty();
}

Names StorageInMemoryMetadata::getColumnsRequiredForSampling() const
{
    if (hasSamplingKey())
        return sampling_key.expression->getRequiredColumns();
    return {};
}

const KeyDescription & StorageInMemoryMetadata::getPrimaryKey() const
{
    return primary_key;
}

bool StorageInMemoryMetadata::isPrimaryKeyDefined() const
{
    return primary_key.definition_ast != nullptr;
}

bool StorageInMemoryMetadata::hasPrimaryKey() const
{
    return !primary_key.column_names.empty();
}

Names StorageInMemoryMetadata::getColumnsRequiredForPrimaryKey() const
{
    if (hasPrimaryKey())
        return primary_key.expression->getRequiredColumns();
    return {};
}

Names StorageInMemoryMetadata::getPrimaryKeyColumns() const
{
    if (!primary_key.column_names.empty())
        return primary_key.column_names;
    return {};
}

const KeyDescription & StorageInMemoryMetadata::getUniqueKey() const
{
    return unique_key;
}

bool StorageInMemoryMetadata::isUniqueKeyDefined() const
{
    return unique_key.definition_ast != nullptr;
}

bool StorageInMemoryMetadata::hasUniqueKey() const
{
    return !unique_key.column_names.empty();
}

Names StorageInMemoryMetadata::getColumnsRequiredForUniqueKey() const
{
    if (hasUniqueKey())
        return unique_key.expression->getRequiredColumns();
    return {};
}

Names StorageInMemoryMetadata::getUniqueKeyColumns() const
{
    return unique_key.column_names;
}

ExpressionActionsPtr StorageInMemoryMetadata::getUniqueKeyExpression() const
{
    return unique_key.expression;
}

ASTPtr StorageInMemoryMetadata::getSettingsChanges() const
{
    if (settings_changes)
        return settings_changes->clone();
    return nullptr;
}
const SelectQueryDescription & StorageInMemoryMetadata::getSelectQuery() const
{
    return select;
}

bool StorageInMemoryMetadata::hasSelectQuery() const
{
    return select.select_query != nullptr;
}

namespace
{
#if !defined(ARCADIA_BUILD)
    using NamesAndTypesMap = google::dense_hash_map<StringRef, const IDataType *, StringRefHash>;
    using UniqueStrings = google::dense_hash_set<StringRef, StringRefHash>;
#else
    using NamesAndTypesMap = google::sparsehash::dense_hash_map<StringRef, const IDataType *, StringRefHash>;
    using UniqueStrings = google::sparsehash::dense_hash_set<StringRef, StringRefHash>;
#endif

    NamesAndTypesMap getColumnsMap(const NamesAndTypesList & columns)
    {
        NamesAndTypesMap res;
        res.set_empty_key(StringRef());

        for (const auto & column : columns)
            res.insert({column.name, column.type.get()});

        return res;
    }

    UniqueStrings initUniqueStrings()
    {
        UniqueStrings strings;
        strings.set_empty_key(StringRef());
        return strings;
    }

    /*
     * This function checks compatibility of enums. It returns true if:
     * 1. Both types are enums.
     * 2. The first type can represent all possible values of the second one.
     * 3. Both types require the same amount of memory.
     */
    bool isCompatibleEnumTypes(const IDataType * lhs, const IDataType * rhs)
    {
        if (IDataTypeEnum const * enum_type = dynamic_cast<IDataTypeEnum const *>(lhs))
        {
            if (!enum_type->contains(*rhs))
                return false;
            return enum_type->getMaximumSizeOfValueInMemory() == rhs->getMaximumSizeOfValueInMemory();
        }
        return false;
    }
}

String listOfColumns(const NamesAndTypesList & available_columns)
{
    WriteBufferFromOwnString ss;
    for (auto it = available_columns.begin(); it != available_columns.end(); ++it)
    {
        if (it != available_columns.begin())
            ss << ", ";
        ss << it->name;
    }
    return ss.str();
}

void StorageInMemoryMetadata::check(const Names & column_names, const NamesAndTypesList & virtuals, const StorageID & storage_id) const
{
    if (column_names.empty())
    {
        auto list_of_columns = listOfColumns(getColumns().getAllPhysicalWithSubcolumns());
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED,
            "Empty list of columns queried. There are columns: {}", list_of_columns);
    }

    const NamesAndTypesList & func_columns = getFuncColumns();
    const auto virtuals_map = getColumnsMap(virtuals);
    auto unique_names = initUniqueStrings();

    for (const auto & name : column_names)
    {
        if (isMapImplicitKey(name)) continue;

        // ignore checking functional columns
        if (func_columns.contains(name))
            continue;

        bool has_column = getColumns().hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name) || virtuals_map.count(name);

        if (!has_column)
        {
            auto list_of_columns = listOfColumns(getColumns().getAllPhysicalWithSubcolumns());
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                "There is no column with name {} in table {}. There are columns: {}",
                backQuote(name), storage_id.getNameForLogs(), list_of_columns);
        }

        if (unique_names.end() != unique_names.find(name))
            throw Exception(ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE, "Column {} queried more than once", name);

        unique_names.insert(name);
    }
}

void StorageInMemoryMetadata::check(const NamesAndTypesList & provided_columns) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto columns_map = getColumnsMap(available_columns);

    const NamesAndTypesList & func_columns = getFuncColumns();
    auto unique_names = initUniqueStrings();

    for (const NameAndTypePair & column : provided_columns)
    {
        if (isMapImplicitKey(column.name)) continue;

        // ignore checking functional columns
        if (func_columns.contains(column.name))
            continue;

        auto it = columns_map.find(column.name);
        if (columns_map.end() == it)
            throw Exception(
                "There is no column with name " + column.name + ". There are columns: " + listOfColumns(available_columns),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!it->second->hasDynamicSubcolumns()
            && !column.type->equals(*it->second)
            && !isCompatibleEnumTypes(it->second, column.type.get()))
            throw Exception(
                "Type mismatch for column " + column.name + ". Column has type " + it->second->getName() + ", got type "
                    + column.type->getName(),
                ErrorCodes::TYPE_MISMATCH);

        if (unique_names.end() != unique_names.find(column.name))
            throw Exception("Column " + column.name + " queried more than once", ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(column.name);
    }
}

void StorageInMemoryMetadata::check(const NamesAndTypesList & provided_columns, const Names & column_names) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto available_columns_map = getColumnsMap(available_columns);
    const auto & provided_columns_map = getColumnsMap(provided_columns);

    if (column_names.empty())
        throw Exception(
            "Empty list of columns queried. There are columns: " + listOfColumns(available_columns),
            ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

    const NamesAndTypesList & func_columns = getFuncColumns();
    auto unique_names = initUniqueStrings();
    for (const String & name : column_names)
    {
        if (isMapImplicitKey(name)) continue;

        // ignore checking functional columns
        if (func_columns.contains(name))
            continue;

        auto it = provided_columns_map.find(name);
        if (provided_columns_map.end() == it)
            continue;

        auto jt = available_columns_map.find(name);
        if (available_columns_map.end() == jt)
            throw Exception(
                "There is no column with name " + name + ". There are columns: " + listOfColumns(available_columns),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!it->second->hasDynamicSubcolumns()
            && !it->second->equals(*jt->second)
            && !isCompatibleEnumTypes(jt->second, it->second))
            throw Exception(
                "Type mismatch for column " + name + ". Column has type " + jt->second->getName() + ", got type " + it->second->getName(),
                ErrorCodes::TYPE_MISMATCH);

        if (unique_names.end() != unique_names.find(name))
            throw Exception("Column " + name + " queried more than once", ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(name);
    }
}

void StorageInMemoryMetadata::check(const Block & block, bool need_all) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto columns_map = getColumnsMap(available_columns);

    NameSet names_in_block;

    block.checkNumberOfRows();

    const NamesAndTypesList & func_columns = getFuncColumns();

    for (const auto & column : block)
    {
        if (isMapImplicitKey(column.name)) continue;

        // ignore checking functional columns
        if (func_columns.contains(column.name))
            continue;

        if (names_in_block.count(column.name))
            throw Exception("Duplicate column " + column.name + " in block", ErrorCodes::DUPLICATE_COLUMN);

        names_in_block.insert(column.name);

        auto it = columns_map.find(column.name);
        if (columns_map.end() == it)
            throw Exception(
                "There is no column with name " + column.name + ". There are columns: " + listOfColumns(available_columns),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!it->second->hasDynamicSubcolumns()
            && !column.type->equals(*it->second)
            && !isCompatibleEnumTypes(it->second, column.type.get()))
            throw Exception(
                "Type mismatch for column " + column.name + ". Column has type " + it->second->getName() + ", got type "
                    + column.type->getName(),
                ErrorCodes::TYPE_MISMATCH);
    }

    if (need_all && names_in_block.size() < columns_map.size())
    {
        for (const auto & available_column : available_columns)
        {
            if (!names_in_block.count(available_column.name))
                throw Exception("Expected column " + available_column.name, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        }
    }
}

}
